/// Numeric message-type identifiers carried in the 4-byte header.
/// Application code should use these constants instead of bare literals.
pub const PING:      u32 = 1;
pub const PONG:      u32 = 2;
pub const JOIN:      u32 = 3;
pub const JOINED:    u32 = 4;
pub const LEAVE:     u32 = 5;
pub const LEFT:      u32 = 6;
pub const SEND:      u32 = 7;
pub const BROADCAST: u32 = 8;
pub const ERROR:     u32 = 9;

/// Human-readable name for logging / diagnostics.
pub fn name(id: u32) -> &'static str {
    match id {
        PING      => "PING",
        PONG      => "PONG",
        JOIN      => "JOIN",
        JOINED    => "JOINED",
        LEAVE     => "LEAVE",
        LEFT      => "LEFT",
        SEND      => "SEND",
        BROADCAST => "BROADCAST",
        ERROR     => "ERROR",
        _         => "UNKNOWN",
    }
}
