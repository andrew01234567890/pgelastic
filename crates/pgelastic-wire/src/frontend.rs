//! Messages a client sends to a server.
//!
//! Deliberately disjoint from [`BackendMessage`](crate::backend::BackendMessage):
//! `'D'`, `'C'`, `'E'` and `'S'` name different messages in each direction, so a
//! single shared enum would silently decode a `Describe` as a `DataRow`.

use bytes::{BufMut, Bytes, BytesMut};

use crate::codec::{RawFrame, Reader, frame, put_count, put_cstr, put_nullable_value};
use crate::error::{Direction, WireError};
use crate::types::{Format, Target};

/// Which of the four `'p'` messages the client is expected to send next.
///
/// `'p'` is the only tag in the protocol whose meaning is not recoverable from
/// the bytes on the wire: `PasswordMessage`, `SASLInitialResponse`,
/// `SASLResponse` and `GSSResponse` all share it, and only the state of the
/// authentication exchange tells them apart. The decoder therefore requires the
/// caller to say which one it asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum AuthState {
    /// After `AuthenticationCleartextPassword` or `AuthenticationMD5Password`.
    #[default]
    Password,
    /// After `AuthenticationSASL`.
    SaslInitial,
    /// After `AuthenticationSASLContinue`.
    SaslContinue,
    /// After `AuthenticationGSS` or `AuthenticationGSSContinue`.
    Gss,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parse {
    pub name: Bytes,
    pub query: Bytes,
    pub param_types: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bind {
    pub portal: Bytes,
    pub statement: Bytes,
    pub param_formats: Vec<Format>,
    /// `None` is SQL NULL, which is not the same as a zero-length value.
    pub params: Vec<Option<Bytes>>,
    pub result_formats: Vec<Format>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Describe {
    pub target: Target,
    pub name: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execute {
    pub portal: Bytes,
    /// `0` means "no limit"; anything else can draw a `PortalSuspended`.
    pub max_rows: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    pub target: Target,
    pub name: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionCall {
    pub oid: u32,
    pub arg_formats: Vec<Format>,
    pub args: Vec<Option<Bytes>>,
    pub result_format: Format,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslInitialResponse {
    pub mechanism: Bytes,
    /// `None` encodes the `-1` length libpq uses for "no initial response".
    pub initial_response: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendMessage {
    Query(Bytes),
    Parse(Parse),
    Bind(Bind),
    Describe(Describe),
    Execute(Execute),
    Close(Close),
    Flush,
    Sync,
    Terminate,
    /// Opaque by design: COPY payloads are relayed, never parsed.
    CopyData(Bytes),
    CopyDone,
    CopyFail(Bytes),
    FunctionCall(FunctionCall),
    /// Cleartext or MD5 password, including its trailing NUL on the wire.
    PasswordMessage(Bytes),
    SaslInitialResponse(SaslInitialResponse),
    SaslResponse(Bytes),
    GssResponse(Bytes),
}

impl FrontendMessage {
    pub fn tag(&self) -> u8 {
        match self {
            Self::Query(_) => b'Q',
            Self::Parse(_) => b'P',
            Self::Bind(_) => b'B',
            Self::Describe(_) => b'D',
            Self::Execute(_) => b'E',
            Self::Close(_) => b'C',
            Self::Flush => b'H',
            Self::Sync => b'S',
            Self::Terminate => b'X',
            Self::CopyData(_) => b'd',
            Self::CopyDone => b'c',
            Self::CopyFail(_) => b'f',
            Self::FunctionCall(_) => b'F',
            Self::PasswordMessage(_)
            | Self::SaslInitialResponse(_)
            | Self::SaslResponse(_)
            | Self::GssResponse(_) => b'p',
        }
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        let tag = self.tag();
        match self {
            Self::Query(sql) => frame(dst, tag, |dst| put_cstr(dst, sql)),
            Self::Parse(m) => frame(dst, tag, |dst| {
                put_cstr(dst, &m.name);
                put_cstr(dst, &m.query);
                put_count(dst, m.param_types.len());
                for oid in &m.param_types {
                    dst.put_u32(*oid);
                }
            }),
            Self::Bind(m) => frame(dst, tag, |dst| m.encode_body(dst)),
            Self::Describe(m) => frame(dst, tag, |dst| {
                dst.put_u8(m.target.as_byte());
                put_cstr(dst, &m.name);
            }),
            Self::Execute(m) => frame(dst, tag, |dst| {
                put_cstr(dst, &m.portal);
                dst.put_i32(m.max_rows);
            }),
            Self::Close(m) => frame(dst, tag, |dst| {
                dst.put_u8(m.target.as_byte());
                put_cstr(dst, &m.name);
            }),
            Self::Flush | Self::Sync | Self::Terminate | Self::CopyDone => frame(dst, tag, |_| {}),
            Self::CopyData(data) => frame(dst, tag, |dst| dst.put_slice(data)),
            Self::CopyFail(message) => frame(dst, tag, |dst| put_cstr(dst, message)),
            Self::FunctionCall(m) => frame(dst, tag, |dst| m.encode_body(dst)),
            Self::PasswordMessage(secret) => frame(dst, tag, |dst| dst.put_slice(secret)),
            Self::SaslInitialResponse(m) => frame(dst, tag, |dst| {
                put_cstr(dst, &m.mechanism);
                put_nullable_value(dst, m.initial_response.as_ref());
            }),
            Self::SaslResponse(data) | Self::GssResponse(data) => {
                frame(dst, tag, |dst| dst.put_slice(data));
            }
        }
    }

    /// Decodes one frame.
    ///
    /// `auth` disambiguates the `'p'` tag and is ignored for every other tag.
    pub fn decode(raw: &RawFrame, auth: AuthState) -> Result<Self, WireError> {
        let mut r = raw.reader();
        let message = match raw.tag {
            b'Q' => {
                let sql = r.cstr()?;
                Self::Query(sql)
            }
            b'P' => Self::Parse(Parse::decode_body(&mut r)?),
            b'B' => Self::Bind(Bind::decode_body(&mut r)?),
            b'D' => Self::Describe(Describe {
                target: Target::from_byte(r.u8()?)?,
                name: r.cstr()?,
            }),
            b'E' => Self::Execute(Execute {
                portal: r.cstr()?,
                max_rows: r.i32()?,
            }),
            b'C' => Self::Close(Close {
                target: Target::from_byte(r.u8()?)?,
                name: r.cstr()?,
            }),
            b'H' => Self::Flush,
            b'S' => Self::Sync,
            b'X' => Self::Terminate,
            b'd' => Self::CopyData(r.rest()),
            b'c' => Self::CopyDone,
            b'f' => Self::CopyFail(r.cstr()?),
            b'F' => Self::FunctionCall(FunctionCall::decode_body(&mut r)?),
            b'p' => match auth {
                AuthState::Password => Self::PasswordMessage(r.rest()),
                AuthState::SaslInitial => Self::SaslInitialResponse(SaslInitialResponse {
                    mechanism: r.cstr()?,
                    initial_response: r.nullable_value()?,
                }),
                AuthState::SaslContinue => Self::SaslResponse(r.rest()),
                AuthState::Gss => Self::GssResponse(r.rest()),
            },
            tag => {
                return Err(WireError::UnknownTag {
                    direction: Direction::Frontend,
                    tag,
                });
            }
        };
        r.end()?;
        Ok(message)
    }
}

impl Parse {
    fn decode_body(r: &mut Reader) -> Result<Self, WireError> {
        let name = r.cstr()?;
        let query = r.cstr()?;
        let declared = usize::try_from(r.i16()?).unwrap_or(0);
        let count = r.count(declared, 4)?;
        let mut param_types = Vec::with_capacity(count);
        for _ in 0..count {
            param_types.push(r.u32()?);
        }
        Ok(Self {
            name,
            query,
            param_types,
        })
    }
}

impl Bind {
    fn encode_body(&self, dst: &mut BytesMut) {
        put_cstr(dst, &self.portal);
        put_cstr(dst, &self.statement);
        put_count(dst, self.param_formats.len());
        for format in &self.param_formats {
            dst.put_i16(format.as_i16());
        }
        put_count(dst, self.params.len());
        for param in &self.params {
            put_nullable_value(dst, param.as_ref());
        }
        put_count(dst, self.result_formats.len());
        for format in &self.result_formats {
            dst.put_i16(format.as_i16());
        }
    }

    fn decode_body(r: &mut Reader) -> Result<Self, WireError> {
        let portal = r.cstr()?;
        let statement = r.cstr()?;
        let param_formats = decode_formats(r)?;
        let declared = usize::try_from(r.i16()?).unwrap_or(0);
        let count = r.count(declared, 4)?;
        let mut params = Vec::with_capacity(count);
        for _ in 0..count {
            params.push(r.nullable_value()?);
        }
        let result_formats = decode_formats(r)?;
        Ok(Self {
            portal,
            statement,
            param_formats,
            params,
            result_formats,
        })
    }
}

impl FunctionCall {
    fn encode_body(&self, dst: &mut BytesMut) {
        dst.put_u32(self.oid);
        put_count(dst, self.arg_formats.len());
        for format in &self.arg_formats {
            dst.put_i16(format.as_i16());
        }
        put_count(dst, self.args.len());
        for arg in &self.args {
            put_nullable_value(dst, arg.as_ref());
        }
        dst.put_i16(self.result_format.as_i16());
    }

    fn decode_body(r: &mut Reader) -> Result<Self, WireError> {
        let oid = r.u32()?;
        let arg_formats = decode_formats(r)?;
        let declared = usize::try_from(r.i16()?).unwrap_or(0);
        let count = r.count(declared, 4)?;
        let mut args = Vec::with_capacity(count);
        for _ in 0..count {
            args.push(r.nullable_value()?);
        }
        Ok(Self {
            oid,
            arg_formats,
            args,
            result_format: Format::from_i16(r.i16()?)?,
        })
    }
}

pub(crate) fn decode_formats(r: &mut Reader) -> Result<Vec<Format>, WireError> {
    let declared = usize::try_from(r.i16()?).unwrap_or(0);
    let count = r.count(declared, 2)?;
    let mut formats = Vec::with_capacity(count);
    for _ in 0..count {
        formats.push(Format::from_i16(r.i16()?)?);
    }
    Ok(formats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::MessageBuffer;

    fn round_trip(message: &FrontendMessage, auth: AuthState) -> FrontendMessage {
        let mut wire = BytesMut::new();
        message.encode(&mut wire);
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&wire);
        let raw = buf.next_frame().unwrap().unwrap();
        assert!(buf.is_empty());
        FrontendMessage::decode(&raw, auth).unwrap()
    }

    #[test]
    fn query_round_trips() {
        let message = FrontendMessage::Query(Bytes::from_static(b"select 1"));
        assert_eq!(round_trip(&message, AuthState::Password), message);
    }

    #[test]
    fn query_decodes_from_captured_bytes() {
        let wire = b"Q\x00\x00\x00\x0dselect 1\x00";
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(wire);
        let raw = buf.next_frame().unwrap().unwrap();
        assert_eq!(
            FrontendMessage::decode(&raw, AuthState::Password).unwrap(),
            FrontendMessage::Query(Bytes::from_static(b"select 1"))
        );
    }

    #[test]
    fn parse_round_trips_with_parameter_types() {
        let message = FrontendMessage::Parse(Parse {
            name: Bytes::from_static(b"s1"),
            query: Bytes::from_static(b"select $1::int"),
            param_types: vec![23],
        });
        assert_eq!(round_trip(&message, AuthState::Password), message);
    }

    #[test]
    fn bind_round_trips_with_a_null_parameter() {
        let message = FrontendMessage::Bind(Bind {
            portal: Bytes::new(),
            statement: Bytes::from_static(b"s1"),
            param_formats: vec![Format::Text, Format::Binary],
            params: vec![Some(Bytes::from_static(b"42")), None],
            result_formats: vec![Format::Binary],
        });
        assert_eq!(round_trip(&message, AuthState::Password), message);
    }

    #[test]
    fn a_null_parameter_is_not_an_empty_parameter() {
        let null = FrontendMessage::Bind(Bind {
            portal: Bytes::new(),
            statement: Bytes::new(),
            param_formats: vec![],
            params: vec![None],
            result_formats: vec![],
        });
        let empty = FrontendMessage::Bind(Bind {
            portal: Bytes::new(),
            statement: Bytes::new(),
            param_formats: vec![],
            params: vec![Some(Bytes::new())],
            result_formats: vec![],
        });
        let mut a = BytesMut::new();
        let mut b = BytesMut::new();
        null.encode(&mut a);
        empty.encode(&mut b);
        assert_ne!(a, b);
        assert_eq!(round_trip(&null, AuthState::Password), null);
        assert_eq!(round_trip(&empty, AuthState::Password), empty);
    }

    #[test]
    fn describe_and_close_carry_the_target_byte() {
        for target in [Target::Statement, Target::Portal] {
            let describe = FrontendMessage::Describe(Describe {
                target,
                name: Bytes::from_static(b"x"),
            });
            let close = FrontendMessage::Close(Close {
                target,
                name: Bytes::from_static(b"x"),
            });
            assert_eq!(round_trip(&describe, AuthState::Password), describe);
            assert_eq!(round_trip(&close, AuthState::Password), close);
        }
    }

    #[test]
    fn describe_rejects_an_invalid_target() {
        let raw = RawFrame::new(b'D', Bytes::from_static(b"Zx\0"));
        assert_eq!(
            FrontendMessage::decode(&raw, AuthState::Password),
            Err(WireError::InvalidTarget(b'Z'))
        );
    }

    #[test]
    fn the_empty_messages_round_trip() {
        for message in [
            FrontendMessage::Flush,
            FrontendMessage::Sync,
            FrontendMessage::Terminate,
            FrontendMessage::CopyDone,
        ] {
            assert_eq!(round_trip(&message, AuthState::Password), message);
        }
    }

    #[test]
    fn terminate_matches_the_captured_bytes() {
        let mut wire = BytesMut::new();
        FrontendMessage::Terminate.encode(&mut wire);
        assert_eq!(&wire[..], b"X\x00\x00\x00\x04");
    }

    #[test]
    fn copy_data_is_opaque() {
        let message = FrontendMessage::CopyData(Bytes::from_static(b"1\t2\n\x00\xff"));
        assert_eq!(round_trip(&message, AuthState::Password), message);
    }

    #[test]
    fn function_call_round_trips() {
        let message = FrontendMessage::FunctionCall(FunctionCall {
            oid: 1_598,
            arg_formats: vec![Format::Binary],
            args: vec![Some(Bytes::from_static(b"\x00\x00\x00\x01")), None],
            result_format: Format::Binary,
        });
        assert_eq!(round_trip(&message, AuthState::Password), message);
    }

    #[test]
    fn p_decodes_by_connection_state() {
        let body = Bytes::from_static(b"SCRAM-SHA-256\x00\x00\x00\x00\x05n,,n=");
        let raw = RawFrame::new(b'p', body.clone());

        assert_eq!(
            FrontendMessage::decode(&raw, AuthState::Password).unwrap(),
            FrontendMessage::PasswordMessage(body.clone())
        );
        assert_eq!(
            FrontendMessage::decode(&raw, AuthState::SaslContinue).unwrap(),
            FrontendMessage::SaslResponse(body.clone())
        );
        assert_eq!(
            FrontendMessage::decode(&raw, AuthState::Gss).unwrap(),
            FrontendMessage::GssResponse(body)
        );
        assert_eq!(
            FrontendMessage::decode(&raw, AuthState::SaslInitial).unwrap(),
            FrontendMessage::SaslInitialResponse(SaslInitialResponse {
                mechanism: Bytes::from_static(b"SCRAM-SHA-256"),
                initial_response: Some(Bytes::from_static(b"n,,n=")),
            })
        );
    }

    #[test]
    fn sasl_initial_response_may_have_no_initial_data() {
        let message = FrontendMessage::SaslInitialResponse(SaslInitialResponse {
            mechanism: Bytes::from_static(b"SCRAM-SHA-256-PLUS"),
            initial_response: None,
        });
        assert_eq!(round_trip(&message, AuthState::SaslInitial), message);
    }

    #[test]
    fn an_unknown_tag_is_an_error_not_a_panic() {
        let raw = RawFrame::new(b'!', Bytes::new());
        assert_eq!(
            FrontendMessage::decode(&raw, AuthState::Password),
            Err(WireError::UnknownTag {
                direction: Direction::Frontend,
                tag: b'!'
            })
        );
    }

    #[test]
    fn a_truncated_body_is_an_error_not_a_panic() {
        let raw = RawFrame::new(b'E', Bytes::from_static(b"portal\x00\x00"));
        assert_eq!(
            FrontendMessage::decode(&raw, AuthState::Password),
            Err(WireError::Truncated)
        );
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let raw = RawFrame::new(b'H', Bytes::from_static(b"junk"));
        assert_eq!(
            FrontendMessage::decode(&raw, AuthState::Password),
            Err(WireError::TrailingBytes(4))
        );
    }

    #[test]
    fn an_implausible_parameter_count_does_not_over_allocate() {
        let raw = RawFrame::new(b'P', Bytes::from_static(b"\x00\x00\x7f\xff"));
        assert_eq!(
            FrontendMessage::decode(&raw, AuthState::Password),
            Err(WireError::ImplausibleCount {
                count: 32_767,
                remaining: 0
            })
        );
    }
}
