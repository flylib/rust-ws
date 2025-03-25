use tokio::sync::{mpsc, oneshot};
use rust_ws::{WebSocketServer, message_handler};

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel::<String>(100);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // 运行 WebSocket 服务器
    let server = WebSocketServer::new("127.0.0.1:8080");
    tokio::spawn(server.run(tx, shutdown_rx));

    // 运行消息处理器
    tokio::spawn(message_handler(rx));

    // 等待退出信号（这里简单等待 60 秒模拟退出）
    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    let _ = shutdown_tx.send(());
}
