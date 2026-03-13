/// Wire format per frame:
///   [msg_id : u32 big-endian]   (4 bytes)
///   [body_len : u32 big-endian] (4 bytes)
///   [body : bytes]              (body_len bytes)
///
/// Sent as a single WebSocket binary message.
use bytes::{Bytes, BytesMut, BufMut};
use prost::Message;
use crate::error::{Result, WsError};

pub const HEADER_LEN: usize = 8;
/// 16 MiB max frame body
pub const MAX_BODY_LEN: usize = 16 * 1024 * 1024;

/// Encode a `prost::Message` into a framed binary payload.
pub fn encode_proto<M: Message>(msg_id: u32, msg: &M) -> Result<Bytes> {
    let body_len = msg.encoded_len();
    let mut buf = BytesMut::with_capacity(HEADER_LEN + body_len);
    buf.put_u32(msg_id);
    buf.put_u32(body_len as u32);
    msg.encode(&mut buf)?;
    Ok(buf.freeze())
}

/// Encode raw bytes (already proto-encoded) into a framed payload.
pub fn encode_raw(msg_id: u32, body: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_LEN + body.len());
    buf.put_u32(msg_id);
    buf.put_u32(body.len() as u32);
    buf.put_slice(body);
    buf.freeze()
}

/// Decode the header from a binary websocket frame.
/// Returns `(msg_id, body_slice)`.
pub fn decode(data: &[u8]) -> Result<(u32, &[u8])> {
    if data.len() < HEADER_LEN {
        return Err(WsError::Incomplete { need: HEADER_LEN, have: data.len() });
    }
    let msg_id = u32::from_be_bytes(data[0..4].try_into().unwrap());
    let len = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;

    if len > MAX_BODY_LEN {
        return Err(WsError::FrameTooLarge(len));
    }
    if data.len() < HEADER_LEN + len {
        return Err(WsError::Incomplete { need: HEADER_LEN + len, have: data.len() });
    }
    Ok((msg_id, &data[HEADER_LEN..HEADER_LEN + len]))
}

/// Decode body bytes into a `prost::Message`.
pub fn decode_proto<M: Message + Default>(data: &[u8]) -> Result<(u32, M)> {
    let (msg_id, body) = decode(data)?;
    let msg = M::decode(body)?;
    Ok((msg_id, msg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Ping;

    #[test]
    fn roundtrip_empty_proto() {
        let ping = Ping {};
        let frame = encode_proto(crate::msg_id::PING, &ping).unwrap();
        assert_eq!(frame.len(), HEADER_LEN + ping.encoded_len());

        let (id, body) = decode(&frame).unwrap();
        assert_eq!(id, crate::msg_id::PING);
        let decoded = Ping::decode(body).unwrap();
        assert_eq!(decoded, ping);
    }

    #[test]
    fn roundtrip_with_fields() {
        use crate::proto::Join;
        let join = Join { room: "lobby".into() };
        let frame = encode_proto(crate::msg_id::JOIN, &join).unwrap();

        let (id, body) = decode(&frame).unwrap();
        assert_eq!(id, crate::msg_id::JOIN);
        let decoded = Join::decode(body).unwrap();
        assert_eq!(decoded.room, "lobby");
    }

    #[test]
    fn incomplete_header_error() {
        let data = [0u8; 4];
        assert!(matches!(decode(&data), Err(WsError::Incomplete { .. })));
    }

    #[test]
    fn frame_too_large_error() {
        let mut data = [0u8; HEADER_LEN];
        // body_len field = MAX_BODY_LEN + 1
        let big = (MAX_BODY_LEN + 1) as u32;
        data[4..8].copy_from_slice(&big.to_be_bytes());
        assert!(matches!(decode(&data), Err(WsError::FrameTooLarge(_))));
    }

    #[test]
    fn encode_raw_roundtrip() {
        let body = b"hello proto";
        let frame = encode_raw(42, body);
        let (id, decoded_body) = decode(&frame).unwrap();
        assert_eq!(id, 42);
        assert_eq!(decoded_body, body);
    }
}
