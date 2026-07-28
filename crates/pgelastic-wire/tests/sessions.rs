use bytes::{Bytes, BytesMut};
use pgelastic_wire::backend::{
    Authentication, BackendKeyData, CopyResponse, DataRow, NegotiateProtocolVersion,
    NotificationResponse, ParameterStatus, RowDescription,
};
use pgelastic_wire::frontend::{Bind, Close, Describe, Execute, Parse};
use pgelastic_wire::startup::{
    PreStartup, PreStartupMachine, PreStartupState, ProtocolVersion, StartupMessage,
    encode_ssl_request,
};
use pgelastic_wire::types::field;
use pgelastic_wire::{
    AuthState, BackendMessage, CancelKey, FieldDescription, Fields, Format, FrontendMessage,
    MessageBuffer, Target, TransactionStatus,
};

fn encode_all_frontend(messages: &[FrontendMessage]) -> BytesMut {
    let mut wire = BytesMut::new();
    for message in messages {
        message.encode(&mut wire);
    }
    wire
}

fn encode_all_backend(messages: &[BackendMessage]) -> BytesMut {
    let mut wire = BytesMut::new();
    for message in messages {
        message.encode(&mut wire);
    }
    wire
}

fn decode_all_frontend(wire: &[u8], auth: AuthState) -> Vec<FrontendMessage> {
    let mut buf = MessageBuffer::new();
    buf.extend_from_slice(wire);
    let mut out = Vec::new();
    while let Some(frame) = buf.next_frame().unwrap() {
        out.push(FrontendMessage::decode(&frame, auth).unwrap());
    }
    assert!(buf.is_empty());
    out
}

fn decode_all_backend(wire: &[u8]) -> Vec<BackendMessage> {
    let mut buf = MessageBuffer::new();
    buf.extend_from_slice(wire);
    let mut out = Vec::new();
    while let Some(frame) = buf.next_frame().unwrap() {
        out.push(BackendMessage::decode(&frame).unwrap());
    }
    assert!(buf.is_empty());
    out
}

#[test]
fn an_ssl_negotiated_scram_login_replays_end_to_end() {
    let mut wire = BytesMut::new();
    encode_ssl_request(&mut wire);
    StartupMessage::new(
        ProtocolVersion::V3_0,
        vec![
            (Bytes::from_static(b"user"), Bytes::from_static(b"alice")),
            (Bytes::from_static(b"database"), Bytes::from_static(b"shop")),
        ],
    )
    .encode(&mut wire);

    let mut buf = MessageBuffer::new();
    buf.extend_from_slice(&wire);
    let mut machine = PreStartupMachine::new();

    assert_eq!(
        machine.step(&mut buf).unwrap(),
        Some(PreStartup::SslRequest)
    );
    assert_eq!(machine.state(), PreStartupState::Encrypted);

    let Some(PreStartup::Startup(startup)) = machine.step(&mut buf).unwrap() else {
        panic!("expected a StartupMessage");
    };
    assert_eq!(startup.get(b"database").unwrap(), "shop");
    assert!(machine.is_done());

    let server = encode_all_backend(&[
        BackendMessage::Authentication(Authentication::Sasl {
            mechanisms: vec![Bytes::from_static(b"SCRAM-SHA-256")],
        }),
        BackendMessage::Authentication(Authentication::SaslContinue(Bytes::from_static(
            b"r=abc,s=c2FsdA==,i=4096",
        ))),
        BackendMessage::Authentication(Authentication::SaslFinal(Bytes::from_static(b"v=proof"))),
        BackendMessage::Authentication(Authentication::Ok),
        BackendMessage::ParameterStatus(ParameterStatus {
            name: Bytes::from_static(b"client_encoding"),
            value: Bytes::from_static(b"UTF8"),
        }),
        BackendMessage::BackendKeyData(BackendKeyData {
            process_id: 12_345,
            key: CancelKey::new(Bytes::from_static(b"sixteen-byte-key")).unwrap(),
        }),
        BackendMessage::ReadyForQuery(TransactionStatus::Idle),
    ]);

    let decoded = decode_all_backend(&server);
    assert_eq!(decoded.len(), 7);
    assert_eq!(
        decoded[6],
        BackendMessage::ReadyForQuery(TransactionStatus::Idle)
    );

    let client = encode_all_frontend(&[FrontendMessage::SaslResponse(Bytes::from_static(
        b"c=biws,r=abc,p=proof",
    ))]);
    assert_eq!(
        decode_all_frontend(&client, AuthState::SaslContinue),
        vec![FrontendMessage::SaslResponse(Bytes::from_static(
            b"c=biws,r=abc,p=proof"
        ))]
    );
}

#[test]
fn an_extended_query_pipeline_replays_end_to_end() {
    let client = encode_all_frontend(&[
        FrontendMessage::Parse(Parse {
            name: Bytes::from_static(b"s1"),
            query: Bytes::from_static(b"select $1::int"),
            param_types: vec![23],
        }),
        FrontendMessage::Bind(Bind {
            portal: Bytes::new(),
            statement: Bytes::from_static(b"s1"),
            param_formats: vec![Format::Text],
            params: vec![Some(Bytes::from_static(b"7"))],
            result_formats: vec![Format::Text],
        }),
        FrontendMessage::Describe(Describe {
            target: Target::Portal,
            name: Bytes::new(),
        }),
        FrontendMessage::Execute(Execute {
            portal: Bytes::new(),
            max_rows: 0,
        }),
        FrontendMessage::Close(Close {
            target: Target::Statement,
            name: Bytes::from_static(b"s1"),
        }),
        FrontendMessage::Flush,
        FrontendMessage::Sync,
    ]);

    let decoded = decode_all_frontend(&client, AuthState::Password);
    assert_eq!(decoded.len(), 7);
    assert_eq!(decoded[5], FrontendMessage::Flush);
    assert_eq!(decoded[6], FrontendMessage::Sync);

    let server = encode_all_backend(&[
        BackendMessage::ParseComplete,
        BackendMessage::BindComplete,
        BackendMessage::ParameterDescription(vec![23]),
        BackendMessage::RowDescription(RowDescription {
            fields: vec![FieldDescription {
                name: Bytes::from_static(b"int4"),
                table_oid: 0,
                column_id: 0,
                type_oid: 23,
                type_size: 4,
                type_modifier: -1,
                format: Format::Text,
            }],
        }),
        BackendMessage::DataRow(DataRow::new(Bytes::from_static(
            b"\x00\x01\x00\x00\x00\x017",
        ))),
        BackendMessage::PortalSuspended,
        BackendMessage::CommandComplete(Bytes::from_static(b"SELECT 1")),
        BackendMessage::CloseComplete,
        BackendMessage::NoData,
        BackendMessage::EmptyQueryResponse,
        BackendMessage::ReadyForQuery(TransactionStatus::Transaction),
    ]);

    let decoded = decode_all_backend(&server);
    assert_eq!(decoded.len(), 11);
    assert_eq!(
        decoded[10],
        BackendMessage::ReadyForQuery(TransactionStatus::Transaction)
    );
    assert!(!TransactionStatus::Transaction.is_releasable());
}

#[test]
fn a_copy_in_session_replays_end_to_end() {
    let server = encode_all_backend(&[BackendMessage::CopyInResponse(CopyResponse {
        format: Format::Text,
        column_formats: vec![Format::Text, Format::Text],
    })]);
    assert_eq!(decode_all_backend(&server).len(), 1);

    let client = encode_all_frontend(&[
        FrontendMessage::CopyData(Bytes::from_static(b"1\tone\n")),
        FrontendMessage::CopyData(Bytes::from_static(b"2\ttwo\n")),
        FrontendMessage::CopyDone,
    ]);
    let decoded = decode_all_frontend(&client, AuthState::Password);
    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded[2], FrontendMessage::CopyDone);

    let failed = encode_all_frontend(&[FrontendMessage::CopyFail(Bytes::from_static(b"aborted"))]);
    assert_eq!(
        decode_all_frontend(&failed, AuthState::Password),
        vec![FrontendMessage::CopyFail(Bytes::from_static(b"aborted"))]
    );
}

#[test]
fn a_copy_out_session_replays_end_to_end() {
    let server = encode_all_backend(&[
        BackendMessage::CopyOutResponse(CopyResponse {
            format: Format::Binary,
            column_formats: vec![Format::Binary],
        }),
        BackendMessage::CopyData(Bytes::from_static(
            b"\x00\x01\x00\x00\x00\x04\xde\xad\xbe\xef",
        )),
        BackendMessage::CopyDone,
        BackendMessage::CommandComplete(Bytes::from_static(b"COPY 1")),
        BackendMessage::ReadyForQuery(TransactionStatus::Idle),
    ]);

    let decoded = decode_all_backend(&server);
    assert_eq!(decoded.len(), 5);
    let BackendMessage::CopyData(payload) = &decoded[1] else {
        panic!("expected CopyData");
    };
    assert_eq!(payload.len(), 10);
}

#[test]
fn copy_both_carries_a_replication_stream() {
    let server = encode_all_backend(&[BackendMessage::CopyBothResponse(CopyResponse {
        format: Format::Text,
        column_formats: vec![],
    })]);
    assert!(matches!(
        decode_all_backend(&server)[0],
        BackendMessage::CopyBothResponse(_)
    ));
}

#[test]
fn a_failed_transaction_reports_status_e_and_is_not_releasable() {
    let server = encode_all_backend(&[
        BackendMessage::ErrorResponse(Fields::new(vec![
            (field::SEVERITY, Bytes::from_static(b"ERROR")),
            (field::CODE, Bytes::from_static(b"42601")),
            (field::MESSAGE, Bytes::from_static(b"syntax error")),
            (field::POSITION, Bytes::from_static(b"1")),
        ])),
        BackendMessage::ReadyForQuery(TransactionStatus::Failed),
    ]);

    let decoded = decode_all_backend(&server);
    let BackendMessage::ErrorResponse(fields) = &decoded[0] else {
        panic!("expected an ErrorResponse");
    };
    assert_eq!(fields.sqlstate().unwrap(), "42601");
    assert_eq!(
        decoded[1],
        BackendMessage::ReadyForQuery(TransactionStatus::Failed)
    );
    assert!(!TransactionStatus::Failed.is_releasable());
}

#[test]
fn a_notification_arrives_between_queries() {
    let server = encode_all_backend(&[BackendMessage::NotificationResponse(
        NotificationResponse {
            process_id: 4_242,
            channel: Bytes::from_static(b"jobs"),
            payload: Bytes::from_static(b""),
        },
    )]);
    assert!(matches!(
        decode_all_backend(&server)[0],
        BackendMessage::NotificationResponse(_)
    ));
}

#[test]
fn a_grease_startup_draws_a_negotiate_that_echoes_unknown_options() {
    let startup = StartupMessage::new(
        ProtocolVersion::GREASE,
        vec![
            (Bytes::from_static(b"user"), Bytes::from_static(b"alice")),
            (
                Bytes::from_static(b"_pq_.something_new"),
                Bytes::from_static(b"1"),
            ),
            (
                Bytes::from_static(b"_pq_.also_unknown"),
                Bytes::from_static(b"2"),
            ),
        ],
    );

    let mut wire = BytesMut::new();
    startup.encode(&mut wire);
    let mut buf = MessageBuffer::new();
    buf.extend_from_slice(&wire);
    let Some(PreStartup::Startup(decoded)) = PreStartupMachine::new().step(&mut buf).unwrap()
    else {
        panic!("expected a StartupMessage");
    };
    assert_eq!(decoded, startup);

    let negotiate = NegotiateProtocolVersion::for_startup(&decoded, 2, &[]);
    assert_eq!(negotiate.newest_minor, 2);
    assert_eq!(
        negotiate.unrecognized_options,
        vec![
            Bytes::from_static(b"_pq_.something_new"),
            Bytes::from_static(b"_pq_.also_unknown"),
        ]
    );

    let server = encode_all_backend(&[BackendMessage::NegotiateProtocolVersion(negotiate)]);
    assert_eq!(decode_all_backend(&server).len(), 1);
}

#[test]
fn a_terminate_ends_the_session() {
    let client = encode_all_frontend(&[FrontendMessage::Terminate]);
    assert_eq!(
        decode_all_frontend(&client, AuthState::Password),
        vec![FrontendMessage::Terminate]
    );
}
