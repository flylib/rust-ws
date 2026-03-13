use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMsg};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    codec,
    error::{Result, WsError},
};

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Inbound message buffer depth.
    pub recv_buffer: usize,
    /// Outbound frame buffer depth.
    pub send_buffer: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self { recv_buffer: 256, send_buffer: 256 }
    }
}

// ── Incoming frame from the server ───────────────────────────────────────────

#[derive(Debug)]
pub struct Frame {
    pub msg_id: u32,
    pub body: Bytes,
}

impl Frame {
    /// Decode body into a concrete proto message.
    pub fn decode_body<M: ProstMessage + Default>(&self) -> Result<M> {
        Ok(M::decode(&*self.body)?)
    }
}

// ── Client ────────────────────────────────────────────────────────────────────

/// Async WebSocket client.
///
/// Internally spawns two tasks (reader + writer) so that `send` and `recv`
/// can be called concurrently from the same async context.
/// Dropping the client sends a WebSocket Close frame before tearing down.
pub struct WsClient {
    out_tx: mpsc::Sender<Bytes>,
    in_rx: mpsc::Receiver<Frame>,
    shutdown: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl WsClient {
    /// Connect to a WebSocket server.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_config(url, ClientConfig::default()).await
    }

    pub async fn connect_with_config(url: &str, cfg: ClientConfig) -> Result<Self> {
        let (ws, _resp) = connect_async(url).await?;
        let (mut ws_tx, mut ws_rx) = ws.split();

        let (out_tx, mut out_rx) = mpsc::channel::<Bytes>(cfg.send_buffer);
        let (in_tx, in_rx) = mpsc::channel::<Frame>(cfg.recv_buffer);

        let shutdown = CancellationToken::new();
        let sd_writer = shutdown.clone();
        let sd_reader = shutdown.clone();

        // Writer task: drain out_rx → ws_tx, then send Close on shutdown
        let writer = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = sd_writer.cancelled() => {
                        let _ = ws_tx.send(WsMsg::Close(None)).await;
                        break;
                    }
                    frame = out_rx.recv() => {
                        match frame {
                            Some(f) => {
                                if let Err(e) = ws_tx.send(WsMsg::Binary(f)).await {
                                    warn!("client writer error: {e}");
                                    break;
                                }
                            }
                            None => {
                                let _ = ws_tx.send(WsMsg::Close(None)).await;
                                break;
                            }
                        }
                    }
                }
            }
            debug!("client writer task stopped");
        });

        // Reader task: ws_rx → in_tx
        let reader = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = sd_reader.cancelled() => break,
                    msg = ws_rx.next() => {
                        match msg {
                            Some(Ok(WsMsg::Binary(ref data))) => {
                                match codec::decode(data) {
                                    Ok((msg_id, body)) => {
                                        let frame = Frame {
                                            msg_id,
                                            body: Bytes::copy_from_slice(body),
                                        };
                                        if in_tx.send(frame).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(e) => warn!("client bad frame: {e}"),
                                }
                            }
                            Some(Ok(WsMsg::Close(_))) | None | Some(Err(_)) => break,
                            Some(Ok(_)) => {}
                        }
                    }
                }
            }
            debug!("client reader task stopped");
        });

        Ok(Self {
            out_tx,
            in_rx,
            shutdown,
            tasks: vec![writer, reader],
        })
    }

    /// Gracefully close the connection (sends a WS Close frame).
    pub async fn close(mut self) {
        self.shutdown.cancel();
        for t in self.tasks.drain(..) {
            let _ = t.await;
        }
    }

    /// Send a `prost::Message` with the given msg_id.
    pub async fn send<M: ProstMessage>(&self, msg_id: u32, msg: &M) -> Result<()> {
        let frame = codec::encode_proto(msg_id, msg)?;
        self.out_tx.send(frame).await.map_err(|_| WsError::ChannelSend)
    }

    /// Send raw bytes (already encoded) with the given msg_id.
    pub async fn send_raw(&self, msg_id: u32, body: &[u8]) -> Result<()> {
        let frame = codec::encode_raw(msg_id, body);
        self.out_tx.send(frame).await.map_err(|_| WsError::ChannelSend)
    }

    /// Receive the next incoming frame.  Returns `None` when the connection closes.
    pub async fn recv(&mut self) -> Option<Frame> {
        self.in_rx.recv().await
    }

    /// Receive exactly one frame of the expected msg_id; returns error on mismatch.
    pub async fn recv_expect(&mut self, expected_id: u32) -> Result<Frame> {
        match self.in_rx.recv().await {
            None => Err(WsError::ConnectionClosed),
            Some(f) if f.msg_id != expected_id => Err(WsError::Incomplete {
                need: expected_id as usize,
                have: f.msg_id as usize,
            }),
            Some(f) => Ok(f),
        }
    }

    /// Number of frames currently queued in the receive buffer.
    pub fn recv_buffered(&self) -> usize {
        self.in_rx.len()
    }
}

impl Drop for WsClient {
    fn drop(&mut self) {
        // Signal shutdown so the writer sends a Close frame.
        // Tasks are fire-and-forget at this point; the runtime cleans them up.
        self.shutdown.cancel();
    }
}
