/// Example: production-style room-chat server.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use prost::Message;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::info;

use sockudo_ws::{
    msg_id,
    proto::{self, *},
    server::{ConnId, ServerConfig, ServerEvent, WsServer},
};

type Rooms = Arc<RwLock<HashMap<String, HashSet<ConnId>>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info,sockudo_ws=debug").init();

    let shutdown = CancellationToken::new();
    let server = WsServer::new(ServerConfig {
        max_connections: 10_000,
        ..Default::default()
    });
    let handle = server.handle();

    let (mut event_rx, _addr, _jh) = server.listen("0.0.0.0:9000", shutdown.clone()).await?;
    info!("server-demo started on :9000");

    let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));

    // Ctrl-C → graceful shutdown
    let sd = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("shutting down…");
        sd.cancel();
    });

    while let Some(event) = event_rx.recv().await {
        match event {
            ServerEvent::Connected { conn_id, addr } => {
                info!("+ {conn_id} from {addr}  (total={})", handle.connection_count());
            }
            ServerEvent::Disconnected { conn_id } => {
                let mut r = rooms.write().await;
                for members in r.values_mut() { members.remove(&conn_id); }
                info!("- {conn_id}  (total={})", handle.connection_count());
            }
            ServerEvent::Message { conn_id, msg_id: id, body } => {
                match id {
                    msg_id::PING => {
                        let _ = handle.send_to(conn_id, msg_id::PONG, &Pong {}).await;
                    }
                    msg_id::JOIN => {
                        if let Ok(m) = Join::decode(&*body) {
                            rooms.write().await.entry(m.room.clone()).or_default().insert(conn_id);
                            let _ = handle.send_to(conn_id, msg_id::JOINED, &Joined { room: m.room }).await;
                        }
                    }
                    msg_id::SEND => {
                        if let Ok(m) = proto::Send::decode(&*body) {
                            let r = rooms.read().await;
                            if let Some(members) = r.get(&m.room) {
                                let bc = Broadcast { room: m.room.clone(), from: conn_id.to_string(), message: m.message.clone() };
                                for &mid in members {
                                    let _ = handle.send_to(mid, msg_id::BROADCAST, &bc).await;
                                }
                            }
                        }
                    }
                    msg_id::LEAVE => {
                        if let Ok(m) = Leave::decode(&*body) {
                            rooms.write().await.entry(m.room.clone()).or_default().remove(&conn_id);
                            let _ = handle.send_to(conn_id, msg_id::LEFT, &Left { room: m.room }).await;
                        }
                    }
                    _ => {
                        let _ = handle.send_to(conn_id, msg_id::ERROR, &proto::Error {
                            reason: format!("unknown msg_id {id}"),
                            code: 400,
                        }).await;
                    }
                }
            }
        }
    }

    info!("server-demo exited");
    Ok(())
}
