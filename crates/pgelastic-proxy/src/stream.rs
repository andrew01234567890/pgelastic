//! Socket adapters shared by the client-facing and backend-facing legs.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A stream that replays already-read bytes before reaching the socket.
///
/// Direct-TLS detection is a one-byte peek, and that byte is the start of the
/// `ClientHello`. It has to reach rustls, so it is pushed back in front of the
/// socket rather than dropped.
#[derive(Debug)]
pub struct Prefixed<S> {
    prefix: Bytes,
    inner: S,
}

impl<S> Prefixed<S> {
    pub fn new(prefix: Bytes, inner: S) -> Self {
        Self { prefix, inner }
    }

    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Drops the replay buffer and returns the socket.
    ///
    /// Only sound where the prefix is known to be empty or deliberately
    /// discarded; [`Prefixed::pending`] is the check.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Bytes still waiting to be replayed ahead of the socket.
    pub fn pending(&self) -> usize {
        self.prefix.len()
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Prefixed<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.prefix.is_empty() {
            let take = this.prefix.len().min(buf.remaining());
            buf.put_slice(&this.prefix[..take]);
            this.prefix.advance(take);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Prefixed<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// The client-facing socket.
///
/// The plain arm is [`Prefixed`] even when nothing was pushed back, so that the
/// direct-TLS and `SSLRequest` paths hand rustls the same type.
pub type ClientStream = MaybeTls<
    Prefixed<tokio::net::TcpStream>,
    tokio_rustls::server::TlsStream<Prefixed<tokio::net::TcpStream>>,
>;

/// The backend-facing socket.
pub type BackendStream =
    MaybeTls<tokio::net::TcpStream, tokio_rustls::client::TlsStream<tokio::net::TcpStream>>;

/// Either leg of a connection, before or after a TLS upgrade.
#[derive(Debug)]
pub enum MaybeTls<P, T> {
    Plain(P),
    Tls(Box<T>),
}

impl<P, T> MaybeTls<P, T> {
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }
}

impl<P, T> AsyncRead for MaybeTls<P, T>
where
    P: AsyncRead + Unpin,
    T: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl<P, T> AsyncWrite for MaybeTls<P, T>
where
    P: AsyncWrite + Unpin,
    T: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn prefix_is_replayed_before_the_socket() {
        let (mut a, b) = tokio::io::duplex(64);
        let mut prefixed = Prefixed::new(Bytes::from_static(b"head"), b);

        tokio::io::AsyncWriteExt::write_all(&mut a, b"tail")
            .await
            .unwrap();

        let mut got = vec![0u8; 8];
        let mut filled = 0;
        while filled < 8 {
            filled += prefixed.read(&mut got[filled..]).await.unwrap();
        }
        assert_eq!(&got, b"headtail");
    }
}
