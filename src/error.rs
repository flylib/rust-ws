use thiserror::Error;

#[derive(Debug, Error)]
pub enum WsError {
    #[error("codec: incomplete frame (need {need}, have {have})")]
    Incomplete { need: usize, have: usize },

    #[error("codec: frame body too large ({0} bytes)")]
    FrameTooLarge(usize),

    #[error("codec: zero-length body")]
    EmptyBody,

    #[error("proto decode: {0}")]
    ProtoDecode(#[from] prost::DecodeError),

    #[error("proto encode: {0}")]
    ProtoEncode(#[from] prost::EncodeError),

    #[error("websocket: {0}")]
    WebSocket(#[from] tungstenite::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("connection closed")]
    ConnectionClosed,

    #[error("connection {0} not found")]
    ConnectionNotFound(u64),

    #[error("channel send failed")]
    ChannelSend,

    #[error("server already started")]
    AlreadyStarted,
}

pub type Result<T, E = WsError> = std::result::Result<T, E>;
