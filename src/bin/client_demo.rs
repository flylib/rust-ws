/// Example: interactive CLI client.
use sockudo_ws::{client::WsClient, msg_id, proto::*};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let url = std::env::args().nth(1).unwrap_or_else(|| "ws://127.0.0.1:9000".into());
    let mut client = WsClient::connect(&url).await?;
    tracing::info!("connected to {url}");

    // 1. Ping
    client.send(msg_id::PING, &Ping {}).await?;
    if let Some(f) = client.recv().await {
        tracing::info!("← PONG (msg_id={})", f.msg_id);
    }

    // 2. Join a room
    client.send(msg_id::JOIN, &Join { room: "demo".into() }).await?;
    if let Some(f) = client.recv().await {
        let j = f.decode_body::<Joined>()?;
        tracing::info!("← JOINED room={}", j.room);
    }

    // 3. Send a message
    client.send(msg_id::SEND, &crate_send("demo", "hello world")).await?;
    if let Some(f) = client.recv().await {
        let b = f.decode_body::<Broadcast>()?;
        tracing::info!("← BROADCAST from={} msg={}", b.from, b.message);
    }

    Ok(())
}

fn crate_send(room: &str, message: &str) -> sockudo_ws::proto::Send {
    sockudo_ws::proto::Send { room: room.into(), message: message.into() }
}
