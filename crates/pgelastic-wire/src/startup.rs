//! The pre-startup phase.
//!
//! Modelled as its own type because these packets are structurally unlike every
//! other message: they carry no type byte, so the only way to tell them apart
//! is a first-byte peek plus a magic Int32 request code.

use bytes::{BufMut, Bytes, BytesMut};

use crate::buffer::MessageBuffer;
use crate::codec::{Reader, put_cstr, untagged_frame};
use crate::error::WireError;
use crate::types::CancelKey;

/// The server's own cap on a startup packet, `MAX_STARTUP_PACKET_LENGTH`.
pub const MAX_STARTUP_PACKET_LENGTH: usize = 10000;

/// First byte of a TLS `ClientHello` record: a `PostgreSQL` 17+ client may open
/// the TLS session directly, with no `SSLRequest` round trip.
pub const TLS_HANDSHAKE_FIRST_BYTE: u8 = 0x16;

/// ALPN protocol identifier that direct-TLS connections must negotiate.
///
/// Mandatory, not advisory: without it a direct-TLS listener is an ALPACA-style
/// cross-protocol confusion target, so a client that omits it is rejected.
pub const DIRECT_TLS_ALPN: &[u8] = b"postgresql";

/// Upper half of every magic request code: `1234 << 16`. A packet whose first
/// Int32 carries it is a request, never a version number.
const REQUEST_CODE_MAJOR: i32 = 1234;

pub const SSL_REQUEST_CODE: i32 = 80_877_103;
pub const GSSENC_REQUEST_CODE: i32 = 80_877_104;
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102;

/// A `major.minor` protocol version, as carried in the startup packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProtocolVersion(pub u32);

impl ProtocolVersion {
    pub const V3_0: Self = Self(196_608);
    pub const V3_2: Self = Self(196_610);

    /// `3.9999`, the deliberately-bogus version libpq sends when
    /// `PG_PROTOCOL_GREASE` is set to keep servers honest about version
    /// negotiation. It must draw a `NegotiateProtocolVersion`, never an error.
    pub const GREASE: Self = Self(206_607);

    pub fn new(major: u16, minor: u16) -> Self {
        Self((u32::from(major) << 16) | u32::from(minor))
    }

    pub fn major(self) -> u16 {
        u16::try_from(self.0 >> 16).unwrap_or(u16::MAX)
    }

    pub fn minor(self) -> u16 {
        u16::try_from(self.0 & 0xffff).unwrap_or(u16::MAX)
    }
}

/// A decoded pre-startup packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreStartup {
    /// A raw TLS `ClientHello`; the socket must be handed to the TLS layer
    /// immediately, requiring ALPN [`DIRECT_TLS_ALPN`].
    DirectTls,
    SslRequest,
    GssEncRequest,
    CancelRequest(CancelRequest),
    Startup(StartupMessage),
}

/// A `CancelRequest`, always arriving on its own fresh connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelRequest {
    pub process_id: i32,
    pub key: CancelKey,
}

impl CancelRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        untagged_frame(dst, |dst| {
            dst.put_i32(CANCEL_REQUEST_CODE);
            dst.put_i32(self.process_id);
            dst.put_slice(self.key.as_bytes());
        });
    }

    fn decode(r: &mut Reader) -> Result<Self, WireError> {
        let process_id = r.i32()?;
        let key = CancelKey::new(r.rest())?;
        Ok(Self { process_id, key })
    }
}

/// The startup packet: protocol version plus the session's parameter set.
///
/// Parameters are kept in wire order — the normalized fingerprint of this list
/// is part of the pool key, so it must survive decode/encode unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupMessage {
    pub version: ProtocolVersion,
    pub parameters: Vec<(Bytes, Bytes)>,
}

impl StartupMessage {
    /// Prefix marking a protocol extension parameter.
    pub const EXTENSION_PREFIX: &'static [u8] = b"_pq_.";

    pub fn new(version: ProtocolVersion, parameters: Vec<(Bytes, Bytes)>) -> Self {
        Self {
            version,
            parameters,
        }
    }

    pub fn get(&self, name: &[u8]) -> Option<&Bytes> {
        self.parameters
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v)
    }

    /// Every `_pq_.`-prefixed protocol extension the client asked for.
    pub fn extension_parameters(&self) -> impl Iterator<Item = &(Bytes, Bytes)> {
        self.parameters
            .iter()
            .filter(|(k, _)| k.starts_with(Self::EXTENSION_PREFIX))
    }

    /// Settings nested inside the `options` parameter.
    ///
    /// `options` smuggles GUCs past a naive `(user, database)` pool key, so the
    /// admission path has to see through it.
    pub fn nested_options(&self) -> Vec<(Bytes, Bytes)> {
        self.get(b"options")
            .map_or_else(Vec::new, |raw| parse_options(raw))
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        untagged_frame(dst, |dst| {
            dst.put_u32(self.version.0);
            for (key, value) in &self.parameters {
                put_cstr(dst, key);
                put_cstr(dst, value);
            }
            dst.put_u8(0);
        });
    }

    fn decode(version: ProtocolVersion, r: &mut Reader) -> Result<Self, WireError> {
        if version.major() != 3 {
            return Err(WireError::UnsupportedProtocolVersion {
                major: version.major(),
                minor: version.minor(),
            });
        }
        let mut parameters = Vec::new();
        loop {
            let key = r.cstr()?;
            if key.is_empty() {
                break;
            }
            parameters.push((key, r.cstr()?));
        }
        Ok(Self {
            version,
            parameters,
        })
    }
}

/// Splits an `options` parameter value into individual settings.
///
/// Follows the server's own `pg_split_opts` handling: whitespace separates
/// tokens, a backslash escapes the next byte, and a setting is spelled
/// `-c name=value`, `-cname=value` or `--name=value`. Tokens that are neither
/// are dropped rather than rejected, because the server itself ignores flags it
/// has no use for here.
pub fn parse_options(raw: &[u8]) -> Vec<(Bytes, Bytes)> {
    let mut settings = Vec::new();
    let tokens = split_options(raw);
    let mut i = 0;
    while i < tokens.len() {
        let token = &tokens[i];
        i += 1;
        let assignment = if let Some(rest) = token.strip_prefix(b"--") {
            rest.to_vec()
        } else if let Some(rest) = token.strip_prefix(b"-c") {
            if rest.is_empty() {
                let Some(next) = tokens.get(i) else { break };
                i += 1;
                next.clone()
            } else {
                rest.to_vec()
            }
        } else {
            continue;
        };
        let (name, value) = match memchr::memchr(b'=', &assignment) {
            Some(at) => (&assignment[..at], &assignment[at + 1..]),
            None => (&assignment[..], &b""[..]),
        };
        if name.is_empty() {
            continue;
        }
        settings.push((Bytes::copy_from_slice(name), Bytes::copy_from_slice(value)));
    }
    settings
}

fn split_options(raw: &[u8]) -> Vec<Vec<u8>> {
    let mut tokens = Vec::new();
    let mut current = Vec::new();
    let mut started = false;
    let mut escaped = false;
    for &byte in raw {
        if escaped {
            current.push(byte);
            escaped = false;
            continue;
        }
        match byte {
            b'\\' => {
                escaped = true;
                started = true;
            }
            b' ' | b'\t' | b'\n' | b'\r' => {
                if started {
                    tokens.push(std::mem::take(&mut current));
                    started = false;
                }
            }
            other => {
                current.push(other);
                started = true;
            }
        }
    }
    if started {
        tokens.push(current);
    }
    tokens
}

/// Where the pre-startup negotiation currently stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PreStartupState {
    /// Nothing read yet; a direct-TLS `ClientHello` is still possible.
    Initial,
    /// A `GSSENCRequest` was answered; an `SSLRequest` may still follow.
    GssEncAnswered,
    /// Encryption is settled; only a startup or cancel packet may follow.
    Encrypted,
    /// A startup or cancel packet was decoded; the phase is over.
    Done,
}

/// The pre-startup state machine.
///
/// Drives the packet sequence a client may legally send before the protocol has
/// a type byte to work with, and refuses the sequences that are only useful for
/// making a server loop or downgrade.
#[derive(Debug, Clone)]
pub struct PreStartupMachine {
    state: PreStartupState,
}

impl Default for PreStartupMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl PreStartupMachine {
    pub fn new() -> Self {
        Self {
            state: PreStartupState::Initial,
        }
    }

    /// Starts the machine after the TLS layer, where a `ClientHello` peek is
    /// meaningless and further encryption negotiation is not allowed.
    pub fn after_tls() -> Self {
        Self {
            state: PreStartupState::Encrypted,
        }
    }

    pub fn state(&self) -> PreStartupState {
        self.state
    }

    pub fn is_done(&self) -> bool {
        self.state == PreStartupState::Done
    }

    /// Bytes still required before [`step`] can decide.
    ///
    /// [`step`]: PreStartupMachine::step
    pub fn needed(&self, buf: &MessageBuffer) -> usize {
        if self.state == PreStartupState::Initial && buf.peek() == Some(TLS_HANDSHAKE_FIRST_BYTE) {
            return 0;
        }
        buf.needed_startup()
    }

    /// Consumes one pre-startup packet, if a complete one is buffered.
    pub fn step(&mut self, buf: &mut MessageBuffer) -> Result<Option<PreStartup>, WireError> {
        if self.state == PreStartupState::Done {
            return Err(WireError::PreStartupComplete);
        }
        if self.state == PreStartupState::Initial && buf.peek() == Some(TLS_HANDSHAKE_FIRST_BYTE) {
            // The ClientHello belongs to the TLS layer, so it is left in the
            // buffer rather than consumed.
            self.state = PreStartupState::Encrypted;
            return Ok(Some(PreStartup::DirectTls));
        }
        let Some(body) = buf.next_startup_frame()? else {
            return Ok(None);
        };
        let mut r = Reader::new(body);
        let code = r.i32()?;
        match code {
            SSL_REQUEST_CODE => {
                if self.state == PreStartupState::Encrypted {
                    return Err(WireError::RepeatedNegotiation("SSLRequest"));
                }
                r.end()?;
                self.state = PreStartupState::Encrypted;
                Ok(Some(PreStartup::SslRequest))
            }
            GSSENC_REQUEST_CODE => {
                if self.state != PreStartupState::Initial {
                    return Err(WireError::RepeatedNegotiation("GSSENCRequest"));
                }
                r.end()?;
                self.state = PreStartupState::GssEncAnswered;
                Ok(Some(PreStartup::GssEncRequest))
            }
            CANCEL_REQUEST_CODE => {
                let cancel = CancelRequest::decode(&mut r)?;
                self.state = PreStartupState::Done;
                Ok(Some(PreStartup::CancelRequest(cancel)))
            }
            version if version > 0 && version >> 16 != REQUEST_CODE_MAJOR => {
                let startup =
                    StartupMessage::decode(ProtocolVersion(version.cast_unsigned()), &mut r)?;
                r.end()?;
                self.state = PreStartupState::Done;
                Ok(Some(PreStartup::Startup(startup)))
            }
            other => Err(WireError::UnknownStartupCode(other)),
        }
    }
}

/// Encodes an `SSLRequest` packet.
pub fn encode_ssl_request(dst: &mut BytesMut) {
    untagged_frame(dst, |dst| dst.put_i32(SSL_REQUEST_CODE));
}

/// Encodes a `GSSENCRequest` packet.
pub fn encode_gssenc_request(dst: &mut BytesMut) {
    untagged_frame(dst, |dst| dst.put_i32(GSSENC_REQUEST_CODE));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn machine_with(bytes: &[u8]) -> (PreStartupMachine, MessageBuffer) {
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(bytes);
        (PreStartupMachine::new(), buf)
    }

    #[test]
    fn ssl_request_matches_the_captured_bytes() {
        let (mut m, mut buf) = machine_with(&[0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f]);
        assert_eq!(m.step(&mut buf).unwrap(), Some(PreStartup::SslRequest));
        assert_eq!(m.state(), PreStartupState::Encrypted);
    }

    #[test]
    fn gssenc_request_matches_the_captured_bytes() {
        let (mut m, mut buf) = machine_with(&[0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x30]);
        assert_eq!(m.step(&mut buf).unwrap(), Some(PreStartup::GssEncRequest));
        assert_eq!(m.state(), PreStartupState::GssEncAnswered);
    }

    #[test]
    fn gssenc_then_ssl_then_startup_is_the_libpq_sequence() {
        let mut wire = BytesMut::new();
        encode_gssenc_request(&mut wire);
        encode_ssl_request(&mut wire);
        StartupMessage::new(
            ProtocolVersion::V3_0,
            vec![(Bytes::from_static(b"user"), Bytes::from_static(b"alice"))],
        )
        .encode(&mut wire);

        let (mut m, mut buf) = machine_with(&wire);
        assert_eq!(m.step(&mut buf).unwrap(), Some(PreStartup::GssEncRequest));
        assert_eq!(m.step(&mut buf).unwrap(), Some(PreStartup::SslRequest));
        assert!(matches!(
            m.step(&mut buf).unwrap(),
            Some(PreStartup::Startup(_))
        ));
        assert!(m.is_done());
        assert!(matches!(
            m.step(&mut buf),
            Err(WireError::PreStartupComplete)
        ));
    }

    #[test]
    fn a_second_ssl_request_is_refused() {
        let mut wire = BytesMut::new();
        encode_ssl_request(&mut wire);
        encode_ssl_request(&mut wire);
        let (mut m, mut buf) = machine_with(&wire);
        assert_eq!(m.step(&mut buf).unwrap(), Some(PreStartup::SslRequest));
        assert!(matches!(
            m.step(&mut buf),
            Err(WireError::RepeatedNegotiation("SSLRequest"))
        ));
    }

    #[test]
    fn direct_tls_is_detected_without_consuming_the_client_hello() {
        let (mut m, mut buf) = machine_with(&[0x16, 0x03, 0x01, 0x00, 0xff]);
        assert_eq!(m.step(&mut buf).unwrap(), Some(PreStartup::DirectTls));
        assert_eq!(buf.len(), 5);
        assert_eq!(m.state(), PreStartupState::Encrypted);
    }

    #[test]
    fn direct_tls_is_not_looked_for_after_tls() {
        let mut m = PreStartupMachine::after_tls();
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&[0x16, 0x03, 0x01, 0x00, 0xff]);
        assert!(matches!(
            m.step(&mut buf),
            Err(WireError::StartupPacketTooLarge { .. })
        ));
    }

    #[test]
    fn startup_parameters_decode_in_order() {
        let mut wire = BytesMut::new();
        wire.put_i32(0);
        wire.put_i32(196_608);
        wire.put_slice(b"user\0alice\0database\0shop\0options\0-c geqo=off\0\0");
        let len = i32::try_from(wire.len()).unwrap();
        wire[0..4].copy_from_slice(&len.to_be_bytes());

        let (mut m, mut buf) = machine_with(&wire);
        let Some(PreStartup::Startup(startup)) = m.step(&mut buf).unwrap() else {
            panic!("expected a StartupMessage");
        };
        assert_eq!(startup.version, ProtocolVersion::V3_0);
        assert_eq!(startup.parameters.len(), 3);
        assert_eq!(startup.get(b"user").unwrap(), "alice");
        assert_eq!(
            startup.nested_options(),
            vec![(Bytes::from_static(b"geqo"), Bytes::from_static(b"off"))]
        );
    }

    #[test]
    fn protocol_3_2_is_accepted() {
        let mut wire = BytesMut::new();
        StartupMessage::new(ProtocolVersion::V3_2, vec![]).encode(&mut wire);
        let (mut m, mut buf) = machine_with(&wire);
        let Some(PreStartup::Startup(startup)) = m.step(&mut buf).unwrap() else {
            panic!("expected a StartupMessage");
        };
        assert_eq!(startup.version.major(), 3);
        assert_eq!(startup.version.minor(), 2);
    }

    #[test]
    fn grease_version_is_tolerated() {
        let mut wire = BytesMut::new();
        StartupMessage::new(ProtocolVersion::GREASE, vec![]).encode(&mut wire);
        let (mut m, mut buf) = machine_with(&wire);
        let Some(PreStartup::Startup(startup)) = m.step(&mut buf).unwrap() else {
            panic!("expected a StartupMessage");
        };
        assert_eq!(startup.version, ProtocolVersion::new(3, 9999));
    }

    #[test]
    fn protocol_2_is_rejected() {
        let mut wire = BytesMut::new();
        StartupMessage::new(ProtocolVersion::new(2, 0), vec![]).encode(&mut wire);
        let (mut m, mut buf) = machine_with(&wire);
        assert!(matches!(
            m.step(&mut buf),
            Err(WireError::UnsupportedProtocolVersion { major: 2, minor: 0 })
        ));
    }

    #[test]
    fn oversized_startup_packet_is_rejected_before_allocating() {
        let (mut m, mut buf) = machine_with(&20_000i32.to_be_bytes());
        assert!(matches!(
            m.step(&mut buf),
            Err(WireError::StartupPacketTooLarge { len: 20_000 })
        ));
    }

    #[test]
    fn cancel_request_accepts_four_and_256_byte_keys() {
        for len in [4usize, 256] {
            let mut wire = BytesMut::new();
            CancelRequest {
                process_id: 4242,
                key: CancelKey::new(Bytes::from(vec![9u8; len])).unwrap(),
            }
            .encode(&mut wire);
            let (mut m, mut buf) = machine_with(&wire);
            let Some(PreStartup::CancelRequest(cancel)) = m.step(&mut buf).unwrap() else {
                panic!("expected a CancelRequest");
            };
            assert_eq!(cancel.process_id, 4242);
            assert_eq!(cancel.key.len(), len);
        }
    }

    #[test]
    fn cancel_request_rejects_zero_and_300_byte_keys() {
        for len in [0usize, 300] {
            let mut wire = BytesMut::new();
            untagged_frame(&mut wire, |dst| {
                dst.put_i32(CANCEL_REQUEST_CODE);
                dst.put_i32(1);
                dst.put_slice(&vec![0u8; len]);
            });
            let (mut m, mut buf) = machine_with(&wire);
            assert_eq!(
                m.step(&mut buf),
                Err(WireError::InvalidCancelKeyLength(len))
            );
        }
    }

    #[test]
    fn unknown_request_code_is_rejected() {
        let mut wire = BytesMut::new();
        untagged_frame(&mut wire, |dst| dst.put_i32(80_877_105));
        let (mut m, mut buf) = machine_with(&wire);
        assert!(matches!(
            m.step(&mut buf),
            Err(WireError::UnknownStartupCode(80_877_105))
        ));
    }

    #[test]
    fn options_parsing_handles_escapes_and_all_three_spellings() {
        let parsed = parse_options(br"-c geqo=off -cwork_mem=64MB --search_path=a\ b");
        assert_eq!(
            parsed,
            vec![
                (Bytes::from_static(b"geqo"), Bytes::from_static(b"off")),
                (Bytes::from_static(b"work_mem"), Bytes::from_static(b"64MB")),
                (
                    Bytes::from_static(b"search_path"),
                    Bytes::from_static(b"a b")
                ),
            ]
        );
    }

    #[test]
    fn options_parsing_ignores_bare_flags() {
        assert!(parse_options(b"-h somewhere --").is_empty());
    }
}
