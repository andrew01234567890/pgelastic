//! Reading and writing whole protocol messages during the handshake.
//!
//! Only for the handshake: every message there is small and has to be decoded.
//! The passthrough path uses [`FrameRelay`](crate::relay::FrameRelay) instead,
//! which never buffers a whole large frame.

use bytes::{Bytes, BytesMut};
use pgelastic_wire::{
    AuthState, BackendMessage, Fields, FrontendMessage, MessageBuffer, PreStartup,
    PreStartupMachine, RawFrame, field,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{ProxyError, Result};

/// Reads until the pre-startup machine can decide.
pub async fn read_pre_startup<S: AsyncRead + Unpin>(
    io: &mut S,
    buf: &mut MessageBuffer,
    machine: &mut PreStartupMachine,
) -> Result<PreStartup> {
    loop {
        if let Some(packet) = machine.step(buf)? {
            return Ok(packet);
        }
        if io.read_buf(buf.read_target()).await? == 0 {
            return Err(ProxyError::PeerGone);
        }
    }
}

pub async fn read_frame<S: AsyncRead + Unpin>(
    io: &mut S,
    buf: &mut MessageBuffer,
) -> Result<RawFrame> {
    loop {
        if let Some(frame) = buf.next_frame()? {
            return Ok(frame);
        }
        if io.read_buf(buf.read_target()).await? == 0 {
            return Err(ProxyError::PeerGone);
        }
    }
}

pub async fn read_backend_message<S: AsyncRead + Unpin>(
    io: &mut S,
    buf: &mut MessageBuffer,
) -> Result<BackendMessage> {
    let frame = read_frame(io, buf).await?;
    Ok(BackendMessage::decode(&frame)?)
}

pub async fn read_frontend_message<S: AsyncRead + Unpin>(
    io: &mut S,
    buf: &mut MessageBuffer,
    auth: AuthState,
) -> Result<FrontendMessage> {
    let frame = read_frame(io, buf).await?;
    Ok(FrontendMessage::decode(&frame, auth)?)
}

pub async fn write_backend<S: AsyncWrite + Unpin>(
    io: &mut S,
    messages: &[BackendMessage],
) -> Result<()> {
    let mut wire = BytesMut::new();
    for message in messages {
        message.encode(&mut wire);
    }
    io.write_all(&wire).await?;
    io.flush().await?;
    Ok(())
}

pub async fn write_frontend<S: AsyncWrite + Unpin>(
    io: &mut S,
    messages: &[FrontendMessage],
) -> Result<()> {
    let mut wire = BytesMut::new();
    for message in messages {
        message.encode(&mut wire);
    }
    io.write_all(&wire).await?;
    io.flush().await?;
    Ok(())
}

/// Builds a `FATAL` `ErrorResponse`.
pub fn fatal(sqlstate: &str, message: &str) -> BackendMessage {
    let mut fields = Fields::default();
    fields.push(field::SEVERITY, Bytes::from_static(b"FATAL"));
    fields.push(field::SEVERITY_NONLOCALIZED, Bytes::from_static(b"FATAL"));
    fields.push(field::CODE, Bytes::copy_from_slice(sqlstate.as_bytes()));
    fields.push(field::MESSAGE, Bytes::copy_from_slice(message.as_bytes()));
    BackendMessage::ErrorResponse(fields)
}

/// Best-effort delivery of a fatal error to a client that is about to be
/// dropped. Failures are ignored: the client may already be gone, and there is
/// nothing left to do about it either way.
pub async fn send_fatal<S: AsyncWrite + Unpin>(io: &mut S, sqlstate: &str, message: &str) {
    let _ = write_backend(io, &[fatal(sqlstate, message)]).await;
    let _ = io.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgelastic_wire::TransactionStatus;
    use tokio::io::duplex;

    #[tokio::test]
    async fn a_message_written_here_reads_back_there() {
        let (mut a, mut b) = duplex(1024);
        write_backend(
            &mut a,
            &[BackendMessage::ReadyForQuery(TransactionStatus::Idle)],
        )
        .await
        .unwrap();

        let mut buf = MessageBuffer::new();
        let message = read_backend_message(&mut b, &mut buf).await.unwrap();
        assert_eq!(
            message,
            BackendMessage::ReadyForQuery(TransactionStatus::Idle)
        );
    }

    #[tokio::test]
    async fn a_closed_peer_reports_peer_gone_rather_than_hanging() {
        let (a, mut b) = duplex(64);
        drop(a);
        let mut buf = MessageBuffer::new();
        assert!(matches!(
            read_frame(&mut b, &mut buf).await,
            Err(ProxyError::PeerGone)
        ));
    }

    #[tokio::test]
    async fn a_fatal_error_carries_its_sqlstate() {
        let (mut a, mut b) = duplex(1024);
        send_fatal(&mut a, "28P01", "password authentication failed").await;
        let mut buf = MessageBuffer::new();
        let BackendMessage::ErrorResponse(fields) =
            read_backend_message(&mut b, &mut buf).await.unwrap()
        else {
            panic!("expected an ErrorResponse");
        };
        assert_eq!(fields.sqlstate().unwrap(), "28P01");
        assert_eq!(fields.severity().unwrap(), "FATAL");
    }
}
