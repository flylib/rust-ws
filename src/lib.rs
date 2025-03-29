mod ws_server;
mod ws_conn;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
// 重新导出 WebSocketServer 结构体
pub use ws_server::WebSocketServer;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;

/// 消息处理任务
pub async fn message_handler(mut rx: mpsc::Receiver<String>) {
    while let Some(msg) = rx.recv().await {
        println!("Processing message: {}", msg);
        // 可以在这里进行数据库存储或其他业务逻辑处理
    }
}

pub type ConnectionMap = Arc<Mutex<HashMap<u64, Box<dyn Connection>>>>;

pub fn create_connection_map() -> ConnectionMap {
    Arc::new(Mutex::new(HashMap::new()))
}


trait Connection {
    //同步发送
    fn sync_send(&mut self, msg: String);

    //异步发送
    async fn async_send(&mut self, msg: String);
}