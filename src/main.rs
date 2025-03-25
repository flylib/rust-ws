use futures_util::{SinkExt, StreamExt};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::{mpsc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::accept_async;

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await.expect("Failed to bind");

    // 创建一个队列（mpsc 通道）用于传输 WebSocket 消息
    let (tx, rx) = mpsc::channel::<String>(100);

    // 运行消息处理器（异步任务）
    tokio::spawn(message_handler(rx));

    println!("WebSocket Server running at ws://{}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        let tx = tx.clone();
        tokio::spawn(handle_connection(stream, tx));
    }
}

// 处理 WebSocket 连接
async fn handle_connection(stream: TcpStream, tx: mpsc::Sender<String>) {
    let addr = stream.peer_addr().expect("Failed to get peer address");
    let ws_stream = accept_async(stream).await.expect("Failed to accept websocket");
    println!("New connection: {}", addr);

    let (mut write, mut read) = ws_stream.split();

    while let Some(Ok(msg)) = read.next().await {
        if let Message::Text(text) = msg {
            println!("Received: {} from {}", text, addr);

            // 将消息放入队列
            if let Err(_) = tx.send(text.clone()).await {
                println!("Message queue full, dropping message.");
            }

            // 也可以给客户端回显
            let _ = write.send(Message::Text(format!("Echo: {}", text))).await;
        }
    }

    println!("Connection closed: {}", addr);
}

// 处理队列中的消息
async fn message_handler(mut rx: mpsc::Receiver<String>) {
    while let Some(msg) = rx.recv().await {
        println!("Processing: {}", msg);
        // 这里可以执行数据库操作、业务逻辑处理等
    }
}
