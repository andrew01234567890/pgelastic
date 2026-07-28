use bytes::{Bytes, BytesMut};
use pgelastic_wire::backend::{
    Authentication, BackendKeyData, BackendMessage, CopyResponse, DataRow,
    NegotiateProtocolVersion, NotificationResponse, ParameterStatus, RowDescription,
};
use pgelastic_wire::frontend::{
    AuthState, Bind, Close, Describe, Execute, FrontendMessage, FunctionCall, Parse,
    SaslInitialResponse,
};
use pgelastic_wire::startup::{
    CancelRequest, PreStartup, PreStartupMachine, ProtocolVersion, StartupMessage,
};
use pgelastic_wire::types::{
    CancelKey, FieldDescription, Fields, Format, Target, TransactionStatus,
};
use pgelastic_wire::{DEFAULT_READ_CHUNK, MessageBuffer, RawFrame};
use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Union};

fn text() -> impl Strategy<Value = Bytes> {
    proptest::collection::vec(1u8..=255, 0..24).prop_map(Bytes::from)
}

fn blob() -> impl Strategy<Value = Bytes> {
    proptest::collection::vec(any::<u8>(), 0..48).prop_map(Bytes::from)
}

fn format() -> impl Strategy<Value = Format> {
    prop_oneof![Just(Format::Text), Just(Format::Binary)]
}

fn formats() -> impl Strategy<Value = Vec<Format>> {
    proptest::collection::vec(format(), 0..6)
}

fn nullable_values() -> impl Strategy<Value = Vec<Option<Bytes>>> {
    proptest::collection::vec(proptest::option::of(blob()), 0..6)
}

fn target() -> impl Strategy<Value = Target> {
    prop_oneof![Just(Target::Statement), Just(Target::Portal)]
}

fn cancel_key() -> impl Strategy<Value = CancelKey> {
    proptest::collection::vec(any::<u8>(), CancelKey::MIN_LEN..=CancelKey::MAX_LEN)
        .prop_map(|bytes| CancelKey::new(Bytes::from(bytes)).unwrap())
}

type FrontendCase = BoxedStrategy<(FrontendMessage, AuthState)>;

fn frontend_message() -> impl Strategy<Value = (FrontendMessage, AuthState)> {
    let mut cases = query_messages();
    cases.extend(auth_messages());
    Union::new(cases)
}

fn query_messages() -> Vec<FrontendCase> {
    vec![
        text()
            .prop_map(|q| (FrontendMessage::Query(q), AuthState::Password))
            .boxed(),
        (
            text(),
            text(),
            proptest::collection::vec(any::<u32>(), 0..6),
        )
            .prop_map(|(name, query, param_types)| {
                (
                    FrontendMessage::Parse(Parse {
                        name,
                        query,
                        param_types,
                    }),
                    AuthState::Password,
                )
            })
            .boxed(),
        (text(), text(), formats(), nullable_values(), formats())
            .prop_map(
                |(portal, statement, param_formats, params, result_formats)| {
                    (
                        FrontendMessage::Bind(Bind {
                            portal,
                            statement,
                            param_formats,
                            params,
                            result_formats,
                        }),
                        AuthState::Password,
                    )
                },
            )
            .boxed(),
        (target(), text())
            .prop_map(|(target, name)| {
                (
                    FrontendMessage::Describe(Describe { target, name }),
                    AuthState::Password,
                )
            })
            .boxed(),
        (text(), any::<i32>())
            .prop_map(|(portal, max_rows)| {
                (
                    FrontendMessage::Execute(Execute { portal, max_rows }),
                    AuthState::Password,
                )
            })
            .boxed(),
        (target(), text())
            .prop_map(|(target, name)| {
                (
                    FrontendMessage::Close(Close { target, name }),
                    AuthState::Password,
                )
            })
            .boxed(),
        Just((FrontendMessage::Flush, AuthState::Password)).boxed(),
        Just((FrontendMessage::Sync, AuthState::Password)).boxed(),
        Just((FrontendMessage::Terminate, AuthState::Password)).boxed(),
        Just((FrontendMessage::CopyDone, AuthState::Password)).boxed(),
        blob()
            .prop_map(|d| (FrontendMessage::CopyData(d), AuthState::Password))
            .boxed(),
        text()
            .prop_map(|m| (FrontendMessage::CopyFail(m), AuthState::Password))
            .boxed(),
        (any::<u32>(), formats(), nullable_values(), format())
            .prop_map(|(oid, arg_formats, args, result_format)| {
                (
                    FrontendMessage::FunctionCall(FunctionCall {
                        oid,
                        arg_formats,
                        args,
                        result_format,
                    }),
                    AuthState::Password,
                )
            })
            .boxed(),
    ]
}

fn auth_messages() -> Vec<FrontendCase> {
    vec![
        blob()
            .prop_map(|p| (FrontendMessage::PasswordMessage(p), AuthState::Password))
            .boxed(),
        (text(), proptest::option::of(blob()))
            .prop_map(|(mechanism, initial_response)| {
                (
                    FrontendMessage::SaslInitialResponse(SaslInitialResponse {
                        mechanism,
                        initial_response,
                    }),
                    AuthState::SaslInitial,
                )
            })
            .boxed(),
        blob()
            .prop_map(|d| (FrontendMessage::SaslResponse(d), AuthState::SaslContinue))
            .boxed(),
        blob()
            .prop_map(|d| (FrontendMessage::GssResponse(d), AuthState::Gss))
            .boxed(),
    ]
}

fn authentication() -> impl Strategy<Value = Authentication> {
    Union::new(vec![
        Just(Authentication::Ok).boxed(),
        Just(Authentication::KerberosV5).boxed(),
        Just(Authentication::CleartextPassword).boxed(),
        any::<[u8; 4]>()
            .prop_map(|salt| Authentication::Md5Password { salt })
            .boxed(),
        Just(Authentication::GssApi).boxed(),
        blob().prop_map(Authentication::GssContinue).boxed(),
        Just(Authentication::Sspi).boxed(),
        proptest::collection::vec(text().prop_filter("non-empty", |m| !m.is_empty()), 0..4)
            .prop_map(|mechanisms| Authentication::Sasl { mechanisms })
            .boxed(),
        blob().prop_map(Authentication::SaslContinue).boxed(),
        blob().prop_map(Authentication::SaslFinal).boxed(),
    ])
}

fn field_description() -> impl Strategy<Value = FieldDescription> {
    (
        text(),
        any::<u32>(),
        any::<i16>(),
        any::<u32>(),
        any::<i16>(),
        any::<i32>(),
        format(),
    )
        .prop_map(
            |(name, table_oid, column_id, type_oid, type_size, type_modifier, format)| {
                FieldDescription {
                    name,
                    table_oid,
                    column_id,
                    type_oid,
                    type_size,
                    type_modifier,
                    format,
                }
            },
        )
}

fn fields() -> impl Strategy<Value = Fields> {
    proptest::collection::vec((1u8..=255, text()), 0..6).prop_map(Fields::new)
}

fn copy_response() -> impl Strategy<Value = CopyResponse> {
    (format(), formats()).prop_map(|(format, column_formats)| CopyResponse {
        format,
        column_formats,
    })
}

fn transaction_status() -> impl Strategy<Value = TransactionStatus> {
    prop_oneof![
        Just(TransactionStatus::Idle),
        Just(TransactionStatus::Transaction),
        Just(TransactionStatus::Failed),
    ]
}

fn backend_message() -> impl Strategy<Value = BackendMessage> {
    Union::new(vec![
        authentication()
            .prop_map(BackendMessage::Authentication)
            .boxed(),
        (any::<i32>(), cancel_key())
            .prop_map(|(process_id, key)| {
                BackendMessage::BackendKeyData(BackendKeyData { process_id, key })
            })
            .boxed(),
        (text(), text())
            .prop_map(|(name, value)| {
                BackendMessage::ParameterStatus(ParameterStatus { name, value })
            })
            .boxed(),
        transaction_status()
            .prop_map(BackendMessage::ReadyForQuery)
            .boxed(),
        proptest::collection::vec(field_description(), 0..4)
            .prop_map(|fields| BackendMessage::RowDescription(RowDescription { fields }))
            .boxed(),
        blob()
            .prop_map(|body| BackendMessage::DataRow(DataRow::new(body)))
            .boxed(),
        text().prop_map(BackendMessage::CommandComplete).boxed(),
        Just(BackendMessage::EmptyQueryResponse).boxed(),
        fields().prop_map(BackendMessage::ErrorResponse).boxed(),
        fields().prop_map(BackendMessage::NoticeResponse).boxed(),
        (any::<i32>(), text(), text())
            .prop_map(|(process_id, channel, payload)| {
                BackendMessage::NotificationResponse(NotificationResponse {
                    process_id,
                    channel,
                    payload,
                })
            })
            .boxed(),
        Just(BackendMessage::ParseComplete).boxed(),
        Just(BackendMessage::BindComplete).boxed(),
        Just(BackendMessage::CloseComplete).boxed(),
        Just(BackendMessage::PortalSuspended).boxed(),
        Just(BackendMessage::NoData).boxed(),
        proptest::collection::vec(any::<u32>(), 0..6)
            .prop_map(BackendMessage::ParameterDescription)
            .boxed(),
        copy_response()
            .prop_map(BackendMessage::CopyInResponse)
            .boxed(),
        copy_response()
            .prop_map(BackendMessage::CopyOutResponse)
            .boxed(),
        copy_response()
            .prop_map(BackendMessage::CopyBothResponse)
            .boxed(),
        blob().prop_map(BackendMessage::CopyData).boxed(),
        Just(BackendMessage::CopyDone).boxed(),
        (any::<u32>(), proptest::collection::vec(text(), 0..4))
            .prop_map(|(newest_minor, unrecognized_options)| {
                BackendMessage::NegotiateProtocolVersion(NegotiateProtocolVersion {
                    newest_minor,
                    unrecognized_options,
                })
            })
            .boxed(),
        proptest::option::of(blob())
            .prop_map(BackendMessage::FunctionCallResponse)
            .boxed(),
    ])
}

fn startup_message() -> impl Strategy<Value = StartupMessage> {
    (
        (0u16..=9999u16).prop_map(|minor| ProtocolVersion::new(3, minor)),
        proptest::collection::vec(
            (text().prop_filter("non-empty", |k| !k.is_empty()), text()),
            0..6,
        ),
    )
        .prop_map(|(version, parameters)| StartupMessage::new(version, parameters))
}

fn single_frame(wire: &[u8]) -> RawFrame {
    let mut buf = MessageBuffer::new();
    buf.extend_from_slice(wire);
    let frame = buf.next_frame().unwrap().unwrap();
    assert!(buf.is_empty());
    frame
}

proptest! {
    #[test]
    fn frontend_messages_survive_a_round_trip((message, auth) in frontend_message()) {
        let mut wire = BytesMut::new();
        message.encode(&mut wire);
        let decoded = FrontendMessage::decode(&single_frame(&wire), auth).unwrap();
        prop_assert_eq!(decoded, message);
    }

    #[test]
    fn backend_messages_survive_a_round_trip(message in backend_message()) {
        let mut wire = BytesMut::new();
        message.encode(&mut wire);
        let decoded = BackendMessage::decode(&single_frame(&wire)).unwrap();
        prop_assert_eq!(decoded, message);
    }

    #[test]
    fn startup_messages_survive_a_round_trip(message in startup_message()) {
        let mut wire = BytesMut::new();
        message.encode(&mut wire);
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&wire);
        let decoded = PreStartupMachine::new().step(&mut buf).unwrap().unwrap();
        prop_assert_eq!(decoded, PreStartup::Startup(message));
    }

    #[test]
    fn cancel_requests_survive_a_round_trip(process_id in any::<i32>(), key in cancel_key()) {
        let request = CancelRequest { process_id, key };
        let mut wire = BytesMut::new();
        request.encode(&mut wire);
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&wire);
        let decoded = PreStartupMachine::new().step(&mut buf).unwrap().unwrap();
        prop_assert_eq!(decoded, PreStartup::CancelRequest(request));
    }

    #[test]
    fn raw_frames_re_encode_byte_for_byte(message in backend_message()) {
        let mut wire = BytesMut::new();
        message.encode(&mut wire);
        let frame = single_frame(&wire);
        let mut relayed = BytesMut::new();
        frame.encode(&mut relayed);
        prop_assert_eq!(relayed, wire);
    }

    #[test]
    fn one_byte_at_a_time_matches_one_read(message in backend_message()) {
        let mut wire = BytesMut::new();
        message.encode(&mut wire);

        let whole = single_frame(&wire);

        let mut drip = MessageBuffer::new();
        for (i, byte) in wire.iter().enumerate() {
            prop_assert!(drip.next_frame().unwrap().is_none());
            prop_assert!(drip.needed() > 0);
            prop_assert!(drip.needed() <= wire.len() - i);
            drip.extend_from_slice(&[*byte]);
        }
        prop_assert_eq!(drip.needed(), 0);
        prop_assert_eq!(drip.next_frame().unwrap().unwrap(), whole);
        prop_assert!(drip.is_empty());
    }

    #[test]
    fn arbitrary_chunking_matches_one_read(
        messages in proptest::collection::vec(backend_message(), 1..8),
        chunk in 1usize..=64,
    ) {
        let mut wire = BytesMut::new();
        for message in &messages {
            message.encode(&mut wire);
        }

        let mut chunked = MessageBuffer::new();
        let mut decoded = Vec::new();
        for piece in wire.chunks(chunk) {
            chunked.extend_from_slice(piece);
            while let Some(frame) = chunked.next_frame().unwrap() {
                decoded.push(BackendMessage::decode(&frame).unwrap());
            }
        }
        prop_assert_eq!(decoded, messages);
    }

    #[test]
    fn a_lying_length_never_panics_or_over_allocates(
        tag in any::<u8>(),
        declared in any::<i32>(),
        body in blob(),
    ) {
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&[tag]);
        wire.extend_from_slice(&declared.to_be_bytes());
        wire.extend_from_slice(&body);

        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&wire);
        let _ = buf.next_frame();
        let capacity = buf.read_target().capacity();
        prop_assert!(capacity <= wire.len() + DEFAULT_READ_CHUNK * 2);
    }

    #[test]
    fn arbitrary_bytes_never_panic_the_decoders(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&bytes);
        while let Ok(Some(frame)) = buf.next_frame() {
            let _ = BackendMessage::decode(&frame);
            let _ = FrontendMessage::decode(&frame, AuthState::Password);
            let _ = FrontendMessage::decode(&frame, AuthState::SaslInitial);
        }

        let mut startup_buf = MessageBuffer::new();
        startup_buf.extend_from_slice(&bytes);
        let mut machine = PreStartupMachine::new();
        while let Ok(Some(_)) = machine.step(&mut startup_buf) {
            if machine.is_done() {
                break;
            }
        }
    }
}
