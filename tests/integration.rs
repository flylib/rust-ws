/// Integration tests: spin up a real in-process server, connect real clients.
///
/// Unit tests (codec, proto) live inside their respective modules.
/// These tests cover:
///   1. Single client ping-pong
///   2. Single client join → send → broadcast
///   3. Batch: N clients connect concurrently and exchange messages
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use prost::Message;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use sockudo_ws::{
    msg_id,
    proto::{self, *},
    server::{ConnId, ServerConfig, ServerEvent, WsServer},
    client::WsClient,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Start a chat server on a random port and run the room handler in the background.
/// Returns `(url, handle, shutdown_token)`.
async fn start_chat_server() -> (String, sockudo_ws::ServerHandle, CancellationToken) {
    let shutdown = CancellationToken::new();
    let server = WsServer::new(ServerConfig::default());
    let handle = server.handle();

    // Bind on port 0 — pass the listener directly to avoid port-reuse races.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("ws://{addr}");

    let (mut event_rx, _jh) = server
        .listen_on(listener, shutdown.clone())
        .await
        .unwrap();

    // Room state: room → set of conn_ids
    type Rooms = Arc<RwLock<HashMap<String, HashSet<ConnId>>>>;
    let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));
    let srv_handle = handle.clone();

    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                ServerEvent::Connected { conn_id, .. } => {
                    tracing::debug!("server: connected {conn_id}");
                }
                ServerEvent::Disconnected { conn_id } => {
                    let mut r = rooms.write().await;
                    for members in r.values_mut() {
                        members.remove(&conn_id);
                    }
                }
                ServerEvent::Message { conn_id, msg_id: id, body } => {
                    match id {
                        msg_id::PING => {
                            let _ = srv_handle.send_to(conn_id, msg_id::PONG, &Pong {}).await;
                        }
                        msg_id::JOIN => {
                            if let Ok(m) = Join::decode(&*body) {
                                rooms.write().await
                                    .entry(m.room.clone())
                                    .or_default()
                                    .insert(conn_id);
                                let _ = srv_handle
                                    .send_to(conn_id, msg_id::JOINED, &Joined { room: m.room })
                                    .await;
                            }
                        }
                        msg_id::SEND => {
                            if let Ok(m) = proto::Send::decode(&*body) {
                                let r = rooms.read().await;
                                if let Some(members) = r.get(&m.room) {
                                    let bc = Broadcast {
                                        room: m.room.clone(),
                                        from: conn_id.to_string(),
                                        message: m.message.clone(),
                                    };
                                    for &mid in members {
                                        let _ = srv_handle
                                            .send_to(mid, msg_id::BROADCAST, &bc)
                                            .await;
                                    }
                                }
                            }
                        }
                        msg_id::LEAVE => {
                            if let Ok(m) = Leave::decode(&*body) {
                                rooms.write().await
                                    .entry(m.room.clone())
                                    .or_default()
                                    .remove(&conn_id);
                                let _ = srv_handle
                                    .send_to(conn_id, msg_id::LEFT, &Left { room: m.room })
                                    .await;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    });

    (url, handle, shutdown)
}

// ── Test 1: single ping-pong ──────────────────────────────────────────────────

#[tokio::test]
async fn test_single_ping_pong() {
    let (url, _handle, shutdown) = start_chat_server().await;

    let mut client = WsClient::connect(&url).await.unwrap();
    client.send(msg_id::PING, &Ping {}).await.unwrap();

    let frame = client.recv().await.expect("expected PONG");
    assert_eq!(frame.msg_id, msg_id::PONG);
    let _pong = frame.decode_body::<Pong>().unwrap();

    shutdown.cancel();
}

// ── Test 2: join → send → broadcast ──────────────────────────────────────────

#[tokio::test]
async fn test_join_send_broadcast() {
    let (url, _handle, shutdown) = start_chat_server().await;

    let mut c1 = WsClient::connect(&url).await.unwrap();
    let mut c2 = WsClient::connect(&url).await.unwrap();

    // Both join the same room
    for c in [&c1, &c2] {
        c.send(msg_id::JOIN, &Join { room: "general".into() }).await.unwrap();
    }
    // Drain JOINED acks
    let j1 = c1.recv().await.unwrap();
    assert_eq!(j1.msg_id, msg_id::JOINED);
    let j2 = c2.recv().await.unwrap();
    assert_eq!(j2.msg_id, msg_id::JOINED);

    // c1 sends a message
    c1.send(msg_id::SEND, &proto::Send {
        room: "general".into(),
        message: "hello from c1".into(),
    }).await.unwrap();

    // Both c1 and c2 should receive the broadcast
    let bc1 = c1.recv().await.unwrap();
    let bc2 = c2.recv().await.unwrap();
    assert_eq!(bc1.msg_id, msg_id::BROADCAST);
    assert_eq!(bc2.msg_id, msg_id::BROADCAST);

    let b1 = bc1.decode_body::<Broadcast>().unwrap();
    let b2 = bc2.decode_body::<Broadcast>().unwrap();
    assert_eq!(b1.message, "hello from c1");
    assert_eq!(b2.message, "hello from c1");
    assert_eq!(b1.room, "general");

    shutdown.cancel();
}

// ── Test 3: leave removes from room ──────────────────────────────────────────

#[tokio::test]
async fn test_leave_room() {
    let (url, _handle, shutdown) = start_chat_server().await;

    let mut c1 = WsClient::connect(&url).await.unwrap();
    let mut c2 = WsClient::connect(&url).await.unwrap();

    for c in [&c1, &c2] {
        c.send(msg_id::JOIN, &Join { room: "tmp".into() }).await.unwrap();
    }
    let _ = c1.recv().await; // JOINED
    let _ = c2.recv().await; // JOINED

    // c2 leaves
    c2.send(msg_id::LEAVE, &Leave { room: "tmp".into() }).await.unwrap();
    let left = c2.recv().await.unwrap();
    assert_eq!(left.msg_id, msg_id::LEFT);

    // c1 sends — only c1 should receive the broadcast
    c1.send(msg_id::SEND, &proto::Send {
        room: "tmp".into(),
        message: "only c1".into(),
    }).await.unwrap();

    let bc = c1.recv().await.unwrap();
    assert_eq!(bc.msg_id, msg_id::BROADCAST);

    // c2 should NOT receive anything (give a short window)
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(c2.recv_buffered(), 0);

    shutdown.cancel();
}

// ── Test 4: batch — N clients connect and ping concurrently ──────────────────

#[tokio::test]
async fn test_batch_concurrent_ping() {
    const N: usize = 50;
    let (url, _handle, shutdown) = start_chat_server().await;

    let tasks: Vec<_> = (0..N)
        .map(|i| {
            let url = url.clone();
            tokio::spawn(async move {
                let mut client = WsClient::connect(&url).await
                    .unwrap_or_else(|e| panic!("client {i} connect failed: {e}"));
                client.send(msg_id::PING, &Ping {}).await
                    .unwrap_or_else(|e| panic!("client {i} send failed: {e}"));
                let frame = client.recv().await
                    .unwrap_or_else(|| panic!("client {i}: no reply"));
                assert_eq!(frame.msg_id, msg_id::PONG, "client {i} expected PONG");
            })
        })
        .collect();

    for t in tasks {
        t.await.unwrap();
    }

    shutdown.cancel();
}

// ── Test 5: batch — N clients join the same room and one sends ───────────────

#[tokio::test]
async fn test_batch_room_broadcast() {
    const N: usize = 20;
    let (url, _handle, shutdown) = start_chat_server().await;

    // Connect all clients and join the room
    let mut clients: Vec<WsClient> = Vec::with_capacity(N);
    for _ in 0..N {
        let mut c = WsClient::connect(&url).await.unwrap();
        c.send(msg_id::JOIN, &Join { room: "batch".into() }).await.unwrap();
        clients.push(c);
    }

    // Drain all JOINED acks
    for c in &mut clients {
        let f = c.recv().await.unwrap();
        assert_eq!(f.msg_id, msg_id::JOINED);
    }

    // First client sends a message
    clients[0].send(msg_id::SEND, &proto::Send {
        room: "batch".into(),
        message: "batch-msg".into(),
    }).await.unwrap();

    // All N clients should receive the BROADCAST
    for (i, c) in clients.iter_mut().enumerate() {
        let f = c.recv().await.unwrap_or_else(|| panic!("client {i} got no broadcast"));
        assert_eq!(f.msg_id, msg_id::BROADCAST, "client {i}");
        let bc = f.decode_body::<Broadcast>().unwrap();
        assert_eq!(bc.message, "batch-msg", "client {i}");
    }

    shutdown.cancel();
}

// ── Test 6: connection count ──────────────────────────────────────────────────

#[tokio::test]
async fn test_connection_count() {
    let (url, handle, shutdown) = start_chat_server().await;

    let c1 = WsClient::connect(&url).await.unwrap();
    let c2 = WsClient::connect(&url).await.unwrap();
    // Give server time to register
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(handle.connection_count(), 2);

    drop(c1);
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(handle.connection_count(), 1);

    drop(c2);
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(handle.connection_count(), 0);

    shutdown.cancel();
}
