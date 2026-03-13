use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::net::SocketAddr;

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_tungstenite::{accept_async, tungstenite::Message as WsMsg};
use tracing::{debug, error, info, warn};

use crate::{
    codec,
    error::{Result, WsError},
};

// ── Types ─────────────────────────────────────────────────────────────────────

pub type ConnId = u64;

/// Events emitted by the server to the application layer.
#[derive(Debug)]
pub enum ServerEvent {
    /// New WebSocket connection accepted.
    Connected { conn_id: ConnId, addr: SocketAddr },
    /// Incoming framed message from a client.
    Message { conn_id: ConnId, msg_id: u32, body: Bytes },
    /// Connection was closed (cleanly or with error).
    Disconnected { conn_id: ConnId },
}

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Maximum number of simultaneous connections. 0 = unlimited.
    pub max_connections: usize,
    /// Channel buffer depth for the event stream.
    pub event_buffer: usize,
    /// Per-connection outbound channel buffer depth.
    pub conn_buffer: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 0,
            event_buffer: 4096,
            conn_buffer: 256,
        }
    }
}

// ── Connection registry ───────────────────────────────────────────────────────

struct ConnEntry {
    tx: mpsc::Sender<Bytes>,
    #[allow(dead_code)]
    addr: SocketAddr,
}

/// Shared handle to the connection registry — cheap to clone.
#[derive(Clone)]
pub struct ServerHandle {
    conns: Arc<DashMap<ConnId, ConnEntry>>,
    conn_count: Arc<AtomicUsize>,
}

impl ServerHandle {
    fn new() -> Self {
        Self {
            conns: Arc::new(DashMap::new()),
            conn_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn insert(&self, id: ConnId, tx: mpsc::Sender<Bytes>, addr: SocketAddr) {
        self.conns.insert(id, ConnEntry { tx, addr });
        self.conn_count.fetch_add(1, Ordering::Relaxed);
    }

    fn remove(&self, id: ConnId) {
        if self.conns.remove(&id).is_some() {
            self.conn_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Send a pre-encoded frame to one connection.
    pub async fn send_raw(&self, conn_id: ConnId, frame: Bytes) -> Result<()> {
        let entry = self.conns.get(&conn_id)
            .ok_or(WsError::ConnectionNotFound(conn_id))?;
        entry.tx.send(frame).await.map_err(|_| WsError::ChannelSend)
    }

    /// Encode and send a `prost::Message` to one connection.
    pub async fn send_to<M: ProstMessage>(
        &self,
        conn_id: ConnId,
        msg_id: u32,
        msg: &M,
    ) -> Result<()> {
        let frame = codec::encode_proto(msg_id, msg)?;
        self.send_raw(conn_id, frame).await
    }

    /// Broadcast a `prost::Message` to every connected client.
    pub async fn broadcast<M: ProstMessage>(&self, msg_id: u32, msg: &M) -> Result<()> {
        let frame = codec::encode_proto(msg_id, msg)?;
        self.broadcast_raw(frame).await
    }

    /// Broadcast a pre-encoded frame to every connected client.
    pub async fn broadcast_raw(&self, frame: Bytes) -> Result<()> {
        let ids: Vec<ConnId> = self.conns.iter().map(|e| *e.key()).collect();
        for id in ids {
            if let Some(entry) = self.conns.get(&id) {
                // Best-effort: skip slow/full senders
                let _ = entry.tx.try_send(frame.clone());
            }
        }
        Ok(())
    }

    /// Number of currently connected clients.
    pub fn connection_count(&self) -> usize {
        self.conn_count.load(Ordering::Relaxed)
    }

    /// Force-disconnect a specific client (closes their outbound channel).
    pub fn disconnect(&self, conn_id: ConnId) {
        self.remove(conn_id);
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

pub struct WsServer {
    config: ServerConfig,
    handle: ServerHandle,
    next_id: Arc<AtomicU64>,
}

impl WsServer {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            handle: ServerHandle::new(),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Returns a cloneable handle for sending to connections.
    pub fn handle(&self) -> ServerHandle {
        self.handle.clone()
    }

    /// Bind to `addr` and start the accept loop.
    /// Returns `(event_rx, bound_addr, join_handle)`.
    pub async fn listen(
        self,
        addr: &str,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(mpsc::Receiver<ServerEvent>, SocketAddr, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind(addr).await?;
        let bound = listener.local_addr()?;
        info!("WsServer listening on {bound}");
        let (rx, jh) = self.listen_on(listener, shutdown).await?;
        Ok((rx, bound, jh))
    }

    /// Start the accept loop on an already-bound `TcpListener`.
    /// Useful in tests where you need to know the port before starting.
    pub async fn listen_on(
        self,
        listener: TcpListener,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(mpsc::Receiver<ServerEvent>, tokio::task::JoinHandle<()>)> {
        let (event_tx, event_rx) = mpsc::channel(self.config.event_buffer);
        let config = self.config.clone();
        let handle = self.handle.clone();
        let next_id = self.next_id.clone();

        let jh = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        info!("WsServer shutdown signal received");
                        break;
                    }
                    accept = listener.accept() => {
                        match accept {
                            Ok((stream, addr)) => {
                                if config.max_connections > 0
                                    && handle.connection_count() >= config.max_connections
                                {
                                    warn!("max_connections reached, rejecting {addr}");
                                    continue;
                                }
                                let conn_id = next_id.fetch_add(1, Ordering::Relaxed);
                                spawn_connection(
                                    conn_id,
                                    addr,
                                    stream,
                                    handle.clone(),
                                    event_tx.clone(),
                                    config.conn_buffer,
                                );
                            }
                            Err(e) => {
                                error!("accept error: {e}");
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok((event_rx, jh))
    }
}

// ── Per-connection task ───────────────────────────────────────────────────────

fn spawn_connection(
    conn_id: ConnId,
    addr: SocketAddr,
    stream: TcpStream,
    handle: ServerHandle,
    event_tx: mpsc::Sender<ServerEvent>,
    buf: usize,
) {
    tokio::spawn(async move {
        if let Err(e) = run_connection(conn_id, addr, stream, handle, event_tx, buf).await {
            debug!("conn {conn_id} closed: {e}");
        }
    });
}

async fn run_connection(
    conn_id: ConnId,
    addr: SocketAddr,
    stream: TcpStream,
    handle: ServerHandle,
    event_tx: mpsc::Sender<ServerEvent>,
    buf: usize,
) -> Result<()> {
    let ws = accept_async(stream).await?;
    let (mut ws_tx, mut ws_rx) = ws.split();

    let (out_tx, mut out_rx) = mpsc::channel::<Bytes>(buf);
    handle.insert(conn_id, out_tx, addr);

    let _ = event_tx.send(ServerEvent::Connected { conn_id, addr }).await;
    debug!("conn {conn_id} connected from {addr}");

    loop {
        tokio::select! {
            biased;

            // Outbound: app → ws
            frame = out_rx.recv() => {
                match frame {
                    Some(f) => ws_tx.send(WsMsg::Binary(f)).await?,
                    None => break, // handle dropped sender → disconnect
                }
            }

            // Inbound: ws → app
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(WsMsg::Binary(data))) => {
                        match codec::decode(&data) {
                            Ok((msg_id, body)) => {
                                let _ = event_tx.send(ServerEvent::Message {
                                    conn_id,
                                    msg_id,
                                    body: Bytes::copy_from_slice(body),
                                }).await;
                            }
                            Err(e) => warn!("conn {conn_id} bad frame: {e}"),
                        }
                    }
                    Some(Ok(WsMsg::Close(_))) | None => break,
                    Some(Ok(WsMsg::Ping(p))) => {
                        ws_tx.send(WsMsg::Pong(p)).await?;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        warn!("conn {conn_id} ws error: {e}");
                        break;
                    }
                }
            }
        }
    }

    handle.remove(conn_id);
    let _ = event_tx.send(ServerEvent::Disconnected { conn_id }).await;
    debug!("conn {conn_id} disconnected");
    Ok(())
}
