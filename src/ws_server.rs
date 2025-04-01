use crate::ws_conn::WebSocketConnection;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

/// WebSocket 服务器结构体
pub struct WebSocketServer {
    atomic_id: AtomicU64,
    address: String,
    connections: Arc<Mutex<HashMap<u64, WebSocketConnection>>>, // 连接映射
}

impl WebSocketServer {
    /// 创建 WebSocket 服务器
    pub fn new(address: &str) -> Self {
        Self {
            atomic_id: AtomicU64::new(1),
            address: address.to_string(),
            connections: Arc::new(Mutex::new(HashMap::new())),
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
            self.handle_connection(stream, tx_handler).await;
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

        self.atomic_id.fetch_add(1, Ordering::Relaxed); // 原子递增

        let new_conn = WebSocketConnection::new(self.get_one_connection_id(), addr.to_string(), write);

        let connection_id = new_conn.id;
        self.add_connection(new_conn).await;

        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        // 发送消息到队列
                        if let Err(_) = tx_handler.send(text.parse().unwrap()).await {
                            println!("[{}]Message queue full, dropping message.", connection_id);
                        }
                    }
                    Ok(Message::Binary(_)) => {}
                    Ok(Message::Ping(_)) => {}
                    Ok(Message::Pong(_)) => {}
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Error reading message from {}: {}", addr, e);
                        // self.remove_connection(connection_id).await;
                        break;
                    }
                }
            }
            println!("Connection closed: {}", addr);
        });
    }

    // 获取连接映射的引用
    pub fn get_connections(&self) -> Arc<Mutex<HashMap<u64, WebSocketConnection>>> {
        self.connections.clone()
    }

    // 添加新的连接
    pub async fn add_connection(&self, conn: WebSocketConnection) {
        let mut connections = self.connections.lock().await;
        connections.insert(conn.id, conn);
    }

    // 移除连接
    pub async fn remove_connection(&self, id: u64) {
        let mut connections = self.connections.lock().await;
        connections.remove(&id);
    }

    pub fn get_one_connection_id(&self) -> u64 {
        // 既读取旧值，又写入新值，保证 Acquire + Release
        self.atomic_id.fetch_add(1, Ordering::AcqRel)
    }
}
