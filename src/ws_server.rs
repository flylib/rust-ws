use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;


/// WebSocket 服务器结构体
pub struct WebSocketServer {
    address: String,
}

impl WebSocketServer {
    /// 创建 WebSocket 服务器
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
        }
    }

    /// 启动 WebSocket 服务器
    pub async fn run(&self, tx_handler: mpsc::Sender<String>, shutdown: oneshot::Receiver<()>) {
        let listener = TcpListener::bind(&self.address).await.expect("Failed to bind server");

        println!("WebSocket Server running at ws://{}", self.address);

        tokio::select! {
                _ = Self::accept_connections(listener, tx_handler) => {},
            _ = shutdown => {
                println!("Shutting down WebSocket server...");
            }
        }
    }

    /// 处理 WebSocket 连接
    async fn accept_connections(listener: TcpListener, tx_handler: mpsc::Sender<String>) {
        while let Ok((stream, _)) = listener.accept().await {
            let tx_handler = tx_handler.clone();
            tokio::spawn(Self::handle_connection(stream, tx_handler));
        }
    }

    /// 处理每个 WebSocket 连接
    async fn handle_connection(stream: TcpStream, tx_handler: mpsc::Sender<String>) {
        let addr = stream.peer_addr().expect("Failed to get peer address");
        let ws_stream = accept_async(stream).await.expect("Failed to accept WebSocket");
        println!("New connection: {}", addr);

        let (mut write, mut read) = ws_stream.split();

        while let Some(Ok(msg)) = read.next().await {
            match msg {
                Message::Text(text) => {
                    // 发送消息到队列
                    if let Err(_) = tx_handler.send(text.parse().unwrap()).await {
                        println!("Message queue full, dropping message.");
                    }
                }
                Message::Binary(text) => {}
                Message::Ping(_) => {}
                _ => {}
            }

            if let Message::Text(text) = msg {
                println!("Received: {} from {}", text, addr);


                // 回显消息
                let _ = write.send(Message::Text(text)).await;
            }
        }

        println!("Connection closed: {}", addr);
    }
}
