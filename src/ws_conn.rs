use crate::Connection;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::WebSocketStream;

pub struct WebSocketConnection {
    id: u64,
    addr: String,
    write: SplitSink<WebSocketStream<TcpStream>, Message>,
}

impl WebSocketConnection {
    pub fn new(addr: String, write: SplitSink<WebSocketStream<TcpStream>, Message>) -> Self {
        WebSocketConnection { id: 0, addr, write }
    }
}

impl Connection for WebSocketConnection {
    // fn sync_send(&mut self, msg: String) -> Result<(), Box<dyn std::error::Error>> {
    //     // Convert to UTF-8 validated message
    //     let utf8_bytes = Utf8Bytes::from(msg);
    //     self.write.send(Message::Text(utf8_bytes.into()))?;
    //     Ok(())
    // }

    fn sync_send(&mut self, msg: String) {
        // ❌ 不能在同步方法中直接调用 `send`，需要用 `block_in_place`
        tokio::task::block_in_place(|| {
            let utf8_bytes = Utf8Bytes::from(msg);
            tokio::runtime::Handle::current().block_on(self.write.send(Message::Text(utf8_bytes)))
        }).unwrap();
    }


    async fn async_send(&mut self, msg: String) -> Result<(), Box<dyn std::error::Error>> {
        let utf8_bytes = Utf8Bytes::from(msg);
        self.write.send(Message::Text(utf8_bytes.into())).await?;
        Ok(())
    }
}
