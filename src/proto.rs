/// Protobuf message definitions using `prost` derive macros.
/// No protoc / build.rs needed — field tags are declared inline.
use prost::Message;

// ── Control ──────────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, Message)]
pub struct Ping {}

#[derive(Clone, PartialEq, Message)]
pub struct Pong {}

// ── Room management ───────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, Message)]
pub struct Join {
    #[prost(string, tag = "1")]
    pub room: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Joined {
    #[prost(string, tag = "1")]
    pub room: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Leave {
    #[prost(string, tag = "1")]
    pub room: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Left {
    #[prost(string, tag = "1")]
    pub room: String,
}

// ── Messaging ─────────────────────────────────────────────────────────────────

/// Client → Server: send a message to a room.
#[derive(Clone, PartialEq, Message)]
pub struct Send {
    #[prost(string, tag = "1")]
    pub room: String,
    #[prost(string, tag = "2")]
    pub message: String,
}

/// Server → Client: message broadcast from a room.
#[derive(Clone, PartialEq, Message)]
pub struct Broadcast {
    #[prost(string, tag = "1")]
    pub room: String,
    /// Sender's connection id (stringified).
    #[prost(string, tag = "2")]
    pub from: String,
    #[prost(string, tag = "3")]
    pub message: String,
}

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, Message)]
pub struct Error {
    #[prost(string, tag = "1")]
    pub reason: String,
    /// Optional numeric code for programmatic handling.
    #[prost(uint32, tag = "2")]
    pub code: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn roundtrip<M: Message + Default + PartialEq>(msg: M) {
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        let decoded = M::decode(buf.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn ping_roundtrip()       { roundtrip(Ping {}); }
    #[test]
    fn pong_roundtrip()       { roundtrip(Pong {}); }
    #[test]
    fn join_roundtrip()       { roundtrip(Join { room: "lobby".into() }); }
    #[test]
    fn joined_roundtrip()     { roundtrip(Joined { room: "main".into() }); }
    #[test]
    fn leave_roundtrip()      { roundtrip(Leave { room: "x".into() }); }
    #[test]
    fn left_roundtrip()       { roundtrip(Left { room: "x".into() }); }
    #[test]
    fn send_roundtrip()       { roundtrip(Send { room: "r".into(), message: "hi".into() }); }
    #[test]
    fn broadcast_roundtrip()  {
        roundtrip(Broadcast { room: "r".into(), from: "1".into(), message: "hi".into() });
    }
    #[test]
    fn error_roundtrip()      { roundtrip(Error { reason: "bad".into(), code: 400 }); }
}
