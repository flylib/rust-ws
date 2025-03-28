use rust_ws::{message_handler, WebSocketServer};
use tokio::sync::{mpsc, oneshot};

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel::<String>(100);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // 运行消息处理器
    tokio::spawn(message_handler(rx));

    // 运行 WebSocket 服务器
    let server = WebSocketServer::new("127.0.0.1:8080");

    server.run(tx, shutdown_rx).await;
    let _ = shutdown_tx.send(());
}
