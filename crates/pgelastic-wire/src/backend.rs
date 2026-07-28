//! Messages a server sends to a client.
//!
//! Deliberately disjoint from [`FrontendMessage`](crate::frontend::FrontendMessage):
//! backend `'D'` is a `DataRow` while frontend `'D'` is a `Describe`, backend
//! `'S'` is a `ParameterStatus` while frontend `'S'` is a `Sync`, and so on.

use bytes::{BufMut, Bytes, BytesMut};

use crate::codec::{RawFrame, Reader, frame, put_count, put_cstr, put_nullable_value};
use crate::error::{Direction, WireError};
use crate::frontend::decode_formats;
use crate::startup::StartupMessage;
use crate::types::{CancelKey, FieldDescription, Fields, Format, TransactionStatus};

/// The `AuthenticationX` family, all of which share the `'R'` tag and are
/// separated only by the Int32 that follows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authentication {
    Ok,
    KerberosV5,
    CleartextPassword,
    Md5Password {
        salt: [u8; 4],
    },
    GssApi,
    GssContinue(Bytes),
    Sspi,
    /// The mechanism names on offer, in preference order.
    Sasl {
        mechanisms: Vec<Bytes>,
    },
    SaslContinue(Bytes),
    SaslFinal(Bytes),
}

impl Authentication {
    fn code(&self) -> i32 {
        match self {
            Self::Ok => 0,
            Self::KerberosV5 => 2,
            Self::CleartextPassword => 3,
            Self::Md5Password { .. } => 5,
            Self::GssApi => 7,
            Self::GssContinue(_) => 8,
            Self::Sspi => 9,
            Self::Sasl { .. } => 10,
            Self::SaslContinue(_) => 11,
            Self::SaslFinal(_) => 12,
        }
    }

    fn encode_body(&self, dst: &mut BytesMut) {
        dst.put_i32(self.code());
        match self {
            Self::Ok | Self::KerberosV5 | Self::CleartextPassword | Self::GssApi | Self::Sspi => {}
            Self::Md5Password { salt } => dst.put_slice(salt),
            Self::GssContinue(data) | Self::SaslContinue(data) | Self::SaslFinal(data) => {
                dst.put_slice(data);
            }
            Self::Sasl { mechanisms } => {
                for mechanism in mechanisms {
                    put_cstr(dst, mechanism);
                }
                dst.put_u8(0);
            }
        }
    }

    fn decode_body(r: &mut Reader) -> Result<Self, WireError> {
        Ok(match r.i32()? {
            0 => Self::Ok,
            2 => Self::KerberosV5,
            3 => Self::CleartextPassword,
            5 => {
                let salt = r.take(4)?;
                Self::Md5Password {
                    salt: [salt[0], salt[1], salt[2], salt[3]],
                }
            }
            7 => Self::GssApi,
            8 => Self::GssContinue(r.rest()),
            9 => Self::Sspi,
            10 => {
                let mut mechanisms = Vec::new();
                loop {
                    let mechanism = r.cstr()?;
                    if mechanism.is_empty() {
                        break;
                    }
                    mechanisms.push(mechanism);
                }
                Self::Sasl { mechanisms }
            }
            11 => Self::SaslContinue(r.rest()),
            12 => Self::SaslFinal(r.rest()),
            other => return Err(WireError::UnknownAuthentication(other)),
        })
    }
}

/// The proxy-minted cancellation credentials handed to a client.
///
/// The key is a byte string, never a `u32`: protocol 3.2 widened it, and
/// pgelastic mints structured keys carrying a routing id so a `CancelRequest`
/// that lands on the wrong replica can still be delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKeyData {
    pub process_id: i32,
    pub key: CancelKey,
}

/// The result-set shape of a query, one entry per column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDescription {
    pub fields: Vec<FieldDescription>,
}

/// One result row, never decoded.
///
/// This is a deliberate property of the codec, not an omission. Row payloads
/// are the overwhelming majority of proxied bytes and nothing in a connection
/// pooler needs to look inside one, so the body is carried as an opaque slice
/// of the original buffer and relayed unchanged. Decoding it would cost a parse
/// per row and buy nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRow {
    body: Bytes,
}

impl DataRow {
    pub fn new(body: Bytes) -> Self {
        Self { body }
    }

    /// The undecoded body, ready to be written straight back out.
    pub fn as_bytes(&self) -> &Bytes {
        &self.body
    }

    pub fn into_bytes(self) -> Bytes {
        self.body
    }
}

/// The shape of a COPY subprotocol, shared by `CopyInResponse`,
/// `CopyOutResponse` and `CopyBothResponse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyResponse {
    pub format: Format,
    pub column_formats: Vec<Format>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterStatus {
    pub name: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationResponse {
    pub process_id: i32,
    pub channel: Bytes,
    pub payload: Bytes,
}

/// The server's answer to a startup packet whose minor version or `_pq_.`
/// options it does not fully support.
///
/// Every unrecognised `_pq_.` option **must** be echoed back rather than
/// rejected. Erroring on unknown extension parameters is precisely the bug that
/// made protocol 3.1 unusable and forced the jump to 3.2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiateProtocolVersion {
    /// Newest minor version supported for the major version the client asked for.
    pub newest_minor: u32,
    pub unrecognized_options: Vec<Bytes>,
}

impl NegotiateProtocolVersion {
    /// Builds the response to a startup packet, echoing every `_pq_.` option
    /// not present in `supported`.
    pub fn for_startup(startup: &StartupMessage, newest_minor: u32, supported: &[&[u8]]) -> Self {
        let unrecognized_options = startup
            .extension_parameters()
            .filter(|(key, _)| !supported.iter().any(|s| s == key))
            .map(|(key, _)| key.clone())
            .collect();
        Self {
            newest_minor,
            unrecognized_options,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendMessage {
    Authentication(Authentication),
    BackendKeyData(BackendKeyData),
    ParameterStatus(ParameterStatus),
    /// The pooling release boundary; see [`TransactionStatus`].
    ReadyForQuery(TransactionStatus),
    RowDescription(RowDescription),
    DataRow(DataRow),
    CommandComplete(Bytes),
    EmptyQueryResponse,
    ErrorResponse(Fields),
    NoticeResponse(Fields),
    NotificationResponse(NotificationResponse),
    ParseComplete,
    BindComplete,
    CloseComplete,
    PortalSuspended,
    NoData,
    ParameterDescription(Vec<u32>),
    CopyInResponse(CopyResponse),
    CopyOutResponse(CopyResponse),
    CopyBothResponse(CopyResponse),
    /// Opaque by design, exactly like [`DataRow`].
    CopyData(Bytes),
    CopyDone,
    NegotiateProtocolVersion(NegotiateProtocolVersion),
    FunctionCallResponse(Option<Bytes>),
}

impl BackendMessage {
    pub fn tag(&self) -> u8 {
        match self {
            Self::Authentication(_) => b'R',
            Self::BackendKeyData(_) => b'K',
            Self::ParameterStatus(_) => b'S',
            Self::ReadyForQuery(_) => b'Z',
            Self::RowDescription(_) => b'T',
            Self::DataRow(_) => b'D',
            Self::CommandComplete(_) => b'C',
            Self::EmptyQueryResponse => b'I',
            Self::ErrorResponse(_) => b'E',
            Self::NoticeResponse(_) => b'N',
            Self::NotificationResponse(_) => b'A',
            Self::ParseComplete => b'1',
            Self::BindComplete => b'2',
            Self::CloseComplete => b'3',
            Self::PortalSuspended => b's',
            Self::NoData => b'n',
            Self::ParameterDescription(_) => b't',
            Self::CopyInResponse(_) => b'G',
            Self::CopyOutResponse(_) => b'H',
            Self::CopyBothResponse(_) => b'W',
            Self::CopyData(_) => b'd',
            Self::CopyDone => b'c',
            Self::NegotiateProtocolVersion(_) => b'v',
            Self::FunctionCallResponse(_) => b'V',
        }
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        let tag = self.tag();
        match self {
            Self::Authentication(auth) => frame(dst, tag, |dst| auth.encode_body(dst)),
            Self::BackendKeyData(m) => frame(dst, tag, |dst| {
                dst.put_i32(m.process_id);
                dst.put_slice(m.key.as_bytes());
            }),
            Self::ParameterStatus(m) => frame(dst, tag, |dst| {
                put_cstr(dst, &m.name);
                put_cstr(dst, &m.value);
            }),
            Self::ReadyForQuery(status) => frame(dst, tag, |dst| dst.put_u8(status.as_byte())),
            Self::RowDescription(m) => frame(dst, tag, |dst| {
                put_count(dst, m.fields.len());
                for field in &m.fields {
                    field.encode(dst);
                }
            }),
            Self::DataRow(row) => frame(dst, tag, |dst| dst.put_slice(&row.body)),
            Self::CommandComplete(command_tag) => {
                frame(dst, tag, |dst| put_cstr(dst, command_tag));
            }
            Self::EmptyQueryResponse
            | Self::ParseComplete
            | Self::BindComplete
            | Self::CloseComplete
            | Self::PortalSuspended
            | Self::NoData
            | Self::CopyDone => frame(dst, tag, |_| {}),
            Self::ErrorResponse(fields) | Self::NoticeResponse(fields) => {
                frame(dst, tag, |dst| fields.encode(dst));
            }
            Self::NotificationResponse(m) => frame(dst, tag, |dst| {
                dst.put_i32(m.process_id);
                put_cstr(dst, &m.channel);
                put_cstr(dst, &m.payload);
            }),
            Self::ParameterDescription(oids) => frame(dst, tag, |dst| {
                put_count(dst, oids.len());
                for oid in oids {
                    dst.put_u32(*oid);
                }
            }),
            Self::CopyInResponse(m) | Self::CopyOutResponse(m) | Self::CopyBothResponse(m) => {
                frame(dst, tag, |dst| m.encode_body(dst));
            }
            Self::CopyData(data) => frame(dst, tag, |dst| dst.put_slice(data)),
            Self::NegotiateProtocolVersion(m) => frame(dst, tag, |dst| {
                dst.put_u32(m.newest_minor);
                dst.put_i32(
                    i32::try_from(m.unrecognized_options.len()).expect("option count overflow"),
                );
                for option in &m.unrecognized_options {
                    put_cstr(dst, option);
                }
            }),
            Self::FunctionCallResponse(value) => {
                frame(dst, tag, |dst| put_nullable_value(dst, value.as_ref()));
            }
        }
    }

    pub fn decode(raw: &RawFrame) -> Result<Self, WireError> {
        let mut r = raw.reader();
        let message = match raw.tag {
            b'R' => Self::Authentication(Authentication::decode_body(&mut r)?),
            b'K' => {
                let process_id = r.i32()?;
                Self::BackendKeyData(BackendKeyData {
                    process_id,
                    key: CancelKey::new(r.rest())?,
                })
            }
            b'S' => Self::ParameterStatus(ParameterStatus {
                name: r.cstr()?,
                value: r.cstr()?,
            }),
            b'Z' => Self::ReadyForQuery(TransactionStatus::from_byte(r.u8()?)?),
            b'T' => Self::RowDescription(RowDescription::decode_body(&mut r)?),
            b'D' => Self::DataRow(DataRow { body: r.rest() }),
            b'C' => Self::CommandComplete(r.cstr()?),
            b'I' => Self::EmptyQueryResponse,
            b'E' => Self::ErrorResponse(Fields::decode(&mut r)?),
            b'N' => Self::NoticeResponse(Fields::decode(&mut r)?),
            b'A' => Self::NotificationResponse(NotificationResponse {
                process_id: r.i32()?,
                channel: r.cstr()?,
                payload: r.cstr()?,
            }),
            b'1' => Self::ParseComplete,
            b'2' => Self::BindComplete,
            b'3' => Self::CloseComplete,
            b's' => Self::PortalSuspended,
            b'n' => Self::NoData,
            b't' => {
                let declared = usize::try_from(r.i16()?).unwrap_or(0);
                let count = r.count(declared, 4)?;
                let mut oids = Vec::with_capacity(count);
                for _ in 0..count {
                    oids.push(r.u32()?);
                }
                Self::ParameterDescription(oids)
            }
            b'G' => Self::CopyInResponse(CopyResponse::decode_body(&mut r)?),
            b'H' => Self::CopyOutResponse(CopyResponse::decode_body(&mut r)?),
            b'W' => Self::CopyBothResponse(CopyResponse::decode_body(&mut r)?),
            b'd' => Self::CopyData(r.rest()),
            b'c' => Self::CopyDone,
            b'v' => Self::NegotiateProtocolVersion(NegotiateProtocolVersion::decode_body(&mut r)?),
            b'V' => Self::FunctionCallResponse(r.nullable_value()?),
            tag => {
                return Err(WireError::UnknownTag {
                    direction: Direction::Backend,
                    tag,
                });
            }
        };
        r.end()?;
        Ok(message)
    }
}

impl RowDescription {
    fn decode_body(r: &mut Reader) -> Result<Self, WireError> {
        let declared = usize::try_from(r.i16()?).unwrap_or(0);
        let count = r.count(declared, 19)?;
        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            fields.push(FieldDescription::decode(r)?);
        }
        Ok(Self { fields })
    }
}

impl CopyResponse {
    fn encode_body(&self, dst: &mut BytesMut) {
        dst.put_u8(u8::try_from(self.format.as_i16()).unwrap_or(0));
        put_count(dst, self.column_formats.len());
        for format in &self.column_formats {
            dst.put_i16(format.as_i16());
        }
    }

    fn decode_body(r: &mut Reader) -> Result<Self, WireError> {
        let format = Format::from_i16(i16::from(r.u8()?))?;
        Ok(Self {
            format,
            column_formats: decode_formats(r)?,
        })
    }
}

impl NegotiateProtocolVersion {
    fn decode_body(r: &mut Reader) -> Result<Self, WireError> {
        let newest_minor = r.u32()?;
        let declared = usize::try_from(r.i32()?).unwrap_or(0);
        let count = r.count(declared, 1)?;
        let mut unrecognized_options = Vec::with_capacity(count);
        for _ in 0..count {
            unrecognized_options.push(r.cstr()?);
        }
        Ok(Self {
            newest_minor,
            unrecognized_options,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::MessageBuffer;
    use crate::startup::ProtocolVersion;
    use crate::types::field;

    fn round_trip(message: &BackendMessage) -> BackendMessage {
        let mut wire = BytesMut::new();
        message.encode(&mut wire);
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&wire);
        let raw = buf.next_frame().unwrap().unwrap();
        assert!(buf.is_empty());
        BackendMessage::decode(&raw).unwrap()
    }

    fn decode_all(wire: &[u8]) -> Vec<BackendMessage> {
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(wire);
        let mut out = Vec::new();
        while let Some(raw) = buf.next_frame().unwrap() {
            out.push(BackendMessage::decode(&raw).unwrap());
        }
        out
    }

    #[test]
    fn every_authentication_subtype_round_trips() {
        for auth in [
            Authentication::Ok,
            Authentication::KerberosV5,
            Authentication::CleartextPassword,
            Authentication::Md5Password { salt: [1, 2, 3, 4] },
            Authentication::GssApi,
            Authentication::GssContinue(Bytes::from_static(b"gss")),
            Authentication::Sspi,
            Authentication::Sasl {
                mechanisms: vec![
                    Bytes::from_static(b"SCRAM-SHA-256-PLUS"),
                    Bytes::from_static(b"SCRAM-SHA-256"),
                ],
            },
            Authentication::SaslContinue(Bytes::from_static(b"r=abc,s=def,i=4096")),
            Authentication::SaslFinal(Bytes::from_static(b"v=xyz")),
        ] {
            let message = BackendMessage::Authentication(auth);
            assert_eq!(round_trip(&message), message);
        }
    }

    #[test]
    fn authentication_ok_matches_the_captured_bytes() {
        assert_eq!(
            decode_all(b"R\x00\x00\x00\x08\x00\x00\x00\x00"),
            vec![BackendMessage::Authentication(Authentication::Ok)]
        );
    }

    #[test]
    fn authentication_sasl_matches_the_captured_bytes() {
        assert_eq!(
            decode_all(b"R\x00\x00\x00\x17\x00\x00\x00\x0aSCRAM-SHA-256\x00\x00"),
            vec![BackendMessage::Authentication(Authentication::Sasl {
                mechanisms: vec![Bytes::from_static(b"SCRAM-SHA-256")]
            })]
        );
    }

    #[test]
    fn an_unknown_authentication_code_is_an_error() {
        let raw = RawFrame::new(b'R', Bytes::from_static(b"\x00\x00\x00\x63"));
        assert_eq!(
            BackendMessage::decode(&raw),
            Err(WireError::UnknownAuthentication(99))
        );
    }

    #[test]
    fn ready_for_query_carries_the_transaction_status() {
        for (byte, status) in [
            (b'I', TransactionStatus::Idle),
            (b'T', TransactionStatus::Transaction),
            (b'E', TransactionStatus::Failed),
        ] {
            let wire = [b'Z', 0, 0, 0, 5, byte];
            assert_eq!(
                decode_all(&wire),
                vec![BackendMessage::ReadyForQuery(status)]
            );
            assert_eq!(status.is_releasable(), status == TransactionStatus::Idle);
        }
    }

    #[test]
    fn an_invalid_transaction_status_is_rejected() {
        let raw = RawFrame::new(b'Z', Bytes::from_static(b"X"));
        assert_eq!(
            BackendMessage::decode(&raw),
            Err(WireError::InvalidTransactionStatus(b'X'))
        );
    }

    #[test]
    fn backend_key_data_accepts_the_full_key_length_range() {
        for len in [4usize, 32, 256] {
            let message = BackendMessage::BackendKeyData(BackendKeyData {
                process_id: 7,
                key: CancelKey::new(Bytes::from(vec![3u8; len])).unwrap(),
            });
            assert_eq!(round_trip(&message), message);
        }
    }

    #[test]
    fn backend_key_data_rejects_out_of_range_keys() {
        for len in [0usize, 3, 257, 300] {
            let mut body = BytesMut::new();
            body.put_i32(7);
            body.put_slice(&vec![0u8; len]);
            let raw = RawFrame::new(b'K', body.freeze());
            assert_eq!(
                BackendMessage::decode(&raw),
                Err(WireError::InvalidCancelKeyLength(len))
            );
        }
    }

    #[test]
    fn data_row_is_relayed_without_being_decoded() {
        let body = Bytes::from_static(b"\x00\x02\x00\x00\x00\x01a\xff\xff\xff\xff");
        let message = BackendMessage::DataRow(DataRow::new(body.clone()));
        assert_eq!(round_trip(&message), message);
        let BackendMessage::DataRow(row) = round_trip(&message) else {
            panic!("expected a DataRow");
        };
        assert_eq!(row.as_bytes(), &body);
    }

    #[test]
    fn a_malformed_data_row_still_relays() {
        let raw = RawFrame::new(b'D', Bytes::from_static(b"\xff\xff garbage"));
        assert!(BackendMessage::decode(&raw).is_ok());
    }

    #[test]
    fn row_description_round_trips() {
        let message = BackendMessage::RowDescription(RowDescription {
            fields: vec![FieldDescription {
                name: Bytes::from_static(b"id"),
                table_oid: 16_384,
                column_id: 1,
                type_oid: 23,
                type_size: 4,
                type_modifier: -1,
                format: Format::Text,
            }],
        });
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn an_implausible_column_count_does_not_over_allocate() {
        let raw = RawFrame::new(b'T', Bytes::from_static(b"\x7f\xff"));
        assert_eq!(
            BackendMessage::decode(&raw),
            Err(WireError::ImplausibleCount {
                count: 32_767,
                remaining: 0
            })
        );
    }

    #[test]
    fn error_response_preserves_field_order_and_unknown_fields() {
        let fields = Fields::new(vec![
            (field::SEVERITY, Bytes::from_static(b"ERROR")),
            (field::CODE, Bytes::from_static(b"42P01")),
            (field::MESSAGE, Bytes::from_static(b"no such table")),
            (b'Y', Bytes::from_static(b"from the future")),
        ]);
        let message = BackendMessage::ErrorResponse(fields.clone());
        assert_eq!(round_trip(&message), message);
        assert_eq!(fields.sqlstate().unwrap(), "42P01");
        assert_eq!(fields.message().unwrap(), "no such table");
        assert_eq!(fields.get(b'Y').unwrap(), "from the future");
    }

    #[test]
    fn error_response_matches_the_captured_bytes() {
        let wire = b"E\x00\x00\x00\x13SFATAL\x00C28000\x00\x00";
        let decoded = decode_all(wire);
        let BackendMessage::ErrorResponse(fields) = &decoded[0] else {
            panic!("expected an ErrorResponse");
        };
        assert_eq!(fields.severity().unwrap(), "FATAL");
        assert_eq!(fields.sqlstate().unwrap(), "28000");
    }

    #[test]
    fn notice_response_is_not_an_error_response() {
        let fields = Fields::new(vec![(field::SEVERITY, Bytes::from_static(b"NOTICE"))]);
        let notice = BackendMessage::NoticeResponse(fields.clone());
        let error = BackendMessage::ErrorResponse(fields);
        assert_ne!(notice, error);
        assert_eq!(round_trip(&notice), notice);
        assert_eq!(notice.tag(), b'N');
        assert_eq!(error.tag(), b'E');
    }

    #[test]
    fn the_empty_completions_round_trip() {
        for message in [
            BackendMessage::ParseComplete,
            BackendMessage::BindComplete,
            BackendMessage::CloseComplete,
            BackendMessage::PortalSuspended,
            BackendMessage::NoData,
            BackendMessage::EmptyQueryResponse,
            BackendMessage::CopyDone,
        ] {
            assert_eq!(round_trip(&message), message);
        }
    }

    #[test]
    fn command_complete_round_trips() {
        let message = BackendMessage::CommandComplete(Bytes::from_static(b"INSERT 0 1"));
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn parameter_status_round_trips() {
        let message = BackendMessage::ParameterStatus(ParameterStatus {
            name: Bytes::from_static(b"client_encoding"),
            value: Bytes::from_static(b"UTF8"),
        });
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn notification_response_round_trips() {
        let message = BackendMessage::NotificationResponse(NotificationResponse {
            process_id: 91,
            channel: Bytes::from_static(b"jobs"),
            payload: Bytes::from_static(b"{\"id\":1}"),
        });
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn all_three_copy_responses_round_trip() {
        let body = CopyResponse {
            format: Format::Binary,
            column_formats: vec![Format::Binary, Format::Binary],
        };
        for message in [
            BackendMessage::CopyInResponse(body.clone()),
            BackendMessage::CopyOutResponse(body.clone()),
            BackendMessage::CopyBothResponse(body),
        ] {
            assert_eq!(round_trip(&message), message);
        }
    }

    #[test]
    fn copy_in_and_copy_out_do_not_share_a_tag() {
        assert_ne!(
            BackendMessage::CopyInResponse(CopyResponse {
                format: Format::Text,
                column_formats: vec![]
            })
            .tag(),
            BackendMessage::CopyOutResponse(CopyResponse {
                format: Format::Text,
                column_formats: vec![]
            })
            .tag()
        );
    }

    #[test]
    fn parameter_description_round_trips() {
        let message = BackendMessage::ParameterDescription(vec![23, 25, 1_184]);
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn function_call_response_distinguishes_null_from_empty() {
        let null = BackendMessage::FunctionCallResponse(None);
        let empty = BackendMessage::FunctionCallResponse(Some(Bytes::new()));
        assert_eq!(round_trip(&null), null);
        assert_eq!(round_trip(&empty), empty);
        assert_ne!(null, empty);
    }

    #[test]
    fn negotiate_protocol_version_echoes_unknown_extension_options() {
        let startup = StartupMessage::new(
            ProtocolVersion::new(3, 9999),
            vec![
                (Bytes::from_static(b"user"), Bytes::from_static(b"alice")),
                (
                    Bytes::from_static(b"_pq_.report_parameters"),
                    Bytes::from_static(b"on"),
                ),
                (
                    Bytes::from_static(b"_pq_.made_up_thing"),
                    Bytes::from_static(b"1"),
                ),
            ],
        );
        let negotiate =
            NegotiateProtocolVersion::for_startup(&startup, 0, &[b"_pq_.report_parameters"]);
        assert_eq!(
            negotiate.unrecognized_options,
            vec![Bytes::from_static(b"_pq_.made_up_thing")]
        );

        let message = BackendMessage::NegotiateProtocolVersion(negotiate);
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn negotiate_protocol_version_with_no_unknown_options_is_still_valid() {
        let message = BackendMessage::NegotiateProtocolVersion(NegotiateProtocolVersion {
            newest_minor: 2,
            unrecognized_options: vec![],
        });
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn an_unknown_backend_tag_is_an_error_not_a_panic() {
        let raw = RawFrame::new(b'Q', Bytes::new());
        assert_eq!(
            BackendMessage::decode(&raw),
            Err(WireError::UnknownTag {
                direction: Direction::Backend,
                tag: b'Q'
            })
        );
    }

    #[test]
    fn the_same_tag_means_different_things_in_each_direction() {
        let raw = RawFrame::new(b'S', Bytes::from_static(b"TimeZone\x00UTC\x00"));
        assert!(matches!(
            BackendMessage::decode(&raw).unwrap(),
            BackendMessage::ParameterStatus(_)
        ));

        let sync = RawFrame::new(b'S', Bytes::new());
        assert!(matches!(
            crate::frontend::FrontendMessage::decode(&sync, crate::frontend::AuthState::Password)
                .unwrap(),
            crate::frontend::FrontendMessage::Sync
        ));
    }

    #[test]
    fn a_simple_query_response_sequence_decodes() {
        let mut wire = BytesMut::new();
        BackendMessage::RowDescription(RowDescription {
            fields: vec![FieldDescription {
                name: Bytes::from_static(b"?column?"),
                table_oid: 0,
                column_id: 0,
                type_oid: 23,
                type_size: 4,
                type_modifier: -1,
                format: Format::Text,
            }],
        })
        .encode(&mut wire);
        BackendMessage::DataRow(DataRow::new(Bytes::from_static(
            b"\x00\x01\x00\x00\x00\x011",
        )))
        .encode(&mut wire);
        BackendMessage::CommandComplete(Bytes::from_static(b"SELECT 1")).encode(&mut wire);
        BackendMessage::ReadyForQuery(TransactionStatus::Idle).encode(&mut wire);

        let decoded = decode_all(&wire);
        assert_eq!(decoded.len(), 4);
        assert_eq!(
            decoded[3],
            BackendMessage::ReadyForQuery(TransactionStatus::Idle)
        );
    }
}
