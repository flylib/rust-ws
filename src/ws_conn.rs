use crate::Connection;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

pub struct WebSocketConnection {
    addr: String,
    write: SplitSink<WebSocketStream<TcpStream>, Message>,
}

impl WebSocketConnection {
    pub fn new(addr: String, write: SplitSink<WebSocketStream<TcpStream>, Message>) -> Self {
        WebSocketConnection { addr, write }
    }
}

impl Connection for WebSocketConnection {
    fn sync_send(&mut self, msg: String) -> Result<(), Box<dyn std::error::Error>> {
        // ❌ 不能在同步方法中直接调用 `send`，需要用 `block_in_place`
        let msg = Message::Text(msg);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.write.send(msg))
        })?;
        Ok(())
    }

    async fn async_send(&mut self, msg: String) -> Result<(), Box<dyn std::error::Error>> {
        let msg = Message::Text(msg);
        self.write.send(msg).await?;
        Ok(())
    }
}
