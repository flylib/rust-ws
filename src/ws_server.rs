use crate::ws_conn::WebSocketConnection;
use crate::{create_connection_map, Connection};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

/// WebSocket 服务器结构体
pub struct WebSocketServer {
    address: String,
    connections: Arc<Mutex<HashMap<u64, WebSocketConnection>>>, // 连接映射
}

impl WebSocketServer {
    /// 创建 WebSocket 服务器
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
            connections: create_connection_map(),
        }
    }

    /// 启动 WebSocket 服务器
    pub async fn run(&self, tx_handler: mpsc::Sender<String>, shutdown: oneshot::Receiver<()>) {
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind server");

        println!("WebSocket Server running at ws://{}", self.address);

        tokio::select! {
                _ = self.accept_connections(listener, tx_handler) => {},
            _ = shutdown => {
                println!("Shutting down WebSocket server...");
            }
        }
    }

    /// 处理 WebSocket 连接
    async fn accept_connections(&self, listener: TcpListener, tx_handler: mpsc::Sender<String>) {
        while let Ok((stream, _)) = listener.accept().await {
            let tx_handler = tx_handler.clone();
            tokio::spawn(self.handle_connection(stream, tx_handler));
        }
    }

    /// 处理每个 WebSocket 连接
    async fn handle_connection(&self, stream: TcpStream, tx_handler: mpsc::Sender<String>) {
        let addr = stream.peer_addr().expect("Failed to get peer address");
        let ws_stream = accept_async(stream)
            .await
            .expect("Failed to accept WebSocket");
        println!("New connection: {}", addr);

        let (write, mut read) = ws_stream.split();

        let new_conn = WebSocketConnection::new(addr.to_string(), write);

        let connection_id = new_conn.id;

        self.add_connection(0, Box::new(new_conn)).await;


        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    // 发送消息到队列
                    if let Err(_) = tx_handler.send(text.parse().unwrap()).await {
                        println!("Message queue full, dropping message.");
                    }
                }
                Ok(Message::Binary(_)) => {}
                Ok(Message::Ping(_)) => {}
                Ok(Message::Pong(_)) => {}
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Error reading message from {}: {}", addr, e);
                    self.remove_connection(connection_id).await;
                    break;
                }
            }
        }

        println!("Connection closed: {}", addr);
    }

    // 获取连接映射的引用
    pub fn get_connections(&self) -> Arc<Mutex<HashMap<u64, Box<dyn Connection + Send>>>> {
        self.connections.clone()
    }

    // 添加新的连接
    pub async fn add_connection(&self, id: u64, conn: Box<dyn Connection>) {
        let mut connections = self.connections.lock().await;
        connections.insert(id, conn);
    }

    // 移除连接
    pub async fn remove_connection(&self, id: u64) {
        let mut connections = self.connections.lock().await;
        connections.remove(&id);
    }
}
