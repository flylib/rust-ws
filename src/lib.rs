pub mod codec;
pub mod error;
pub mod msg_id;
pub mod proto;
pub mod server;
pub mod client;

pub use error::{Result, WsError};
pub use server::{ConnId, ServerConfig, ServerEvent, ServerHandle, WsServer};
// Re-export CancellationToken for convenience
pub use tokio_util::sync::CancellationToken;
pub use client::{ClientConfig, Frame, WsClient};
