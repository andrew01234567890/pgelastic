//! Socket adapters shared by the client-facing and backend-facing legs.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// How long a connection may be silent before the kernel starts probing it.
///
/// Both legs of the proxy carry connections that are idle for long stretches by
/// design - a client between transactions, a pooled backend parked for reuse - so
/// this is deliberately well above any of them rather than tuned to a round trip.
const KEEPALIVE_IDLE: std::time::Duration = std::time::Duration::from_secs(60);

/// The gap between probes once one has gone unanswered.
const KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// Unanswered probes before the connection is declared dead, giving roughly 90
/// seconds from the last byte to the socket erroring out.
const KEEPALIVE_RETRIES: u32 = 3;

/// Asks the kernel to notice a peer that stopped answering.
///
/// Without this a peer that vanishes without a FIN - a killed pod, a partitioned
/// node, a NAT that forgot the flow - leaves the socket open indefinitely on this
/// side. On the client leg that is a backend held for a client nobody can reach;
/// on the backend leg it is a checkout that never completes and a pool slot that
/// never comes back. Neither of the two bounds a session is subject to helps: a
/// statement deadline needs the client to have started something, and an
/// idle-in-transaction bound needs a transaction.
///
/// Best effort, like `arm_reset` beside it: a socket the peer has already torn
/// down has nothing to configure, and failing to set an option is not a reason to
/// refuse a connection that otherwise works.
pub fn arm_keepalive(socket: &tokio::net::TcpStream) {
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(KEEPALIVE_IDLE)
        .with_interval(KEEPALIVE_INTERVAL)
        .with_retries(KEEPALIVE_RETRIES);
    let _ = socket2::SockRef::from(socket).set_tcp_keepalive(&keepalive);
}

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

impl BackendStream {
    /// The socket underneath, whether or not TLS was negotiated over it.
    pub fn socket(&self) -> &tokio::net::TcpStream {
        match self {
            Self::Plain(socket) => socket,
            Self::Tls(stream) => stream.get_ref().0,
        }
    }

    /// Severs the connection with an RST rather than a FIN.
    ///
    /// This is the primitive the primary-epoch fence is built on. A graceful
    /// close is a request the peer may take its time honouring, and a demoted
    /// primary finishing one more `COMMIT` while the socket drains is exactly
    /// the write `pg_rewind` is about to discard. A zero linger makes the close
    /// an RST: the kernel discards anything queued and the backend's
    /// `pg_terminate_backend` is not needed for the connection to be gone.
    ///
    /// Best effort by construction — a socket already torn down by the peer has
    /// nothing to reset — and the connection is dropped either way.
    ///
    /// Through `socket2` rather than `TcpStream::set_linger`, which is
    /// deprecated because a *non-zero* linger blocks the closing thread. A zero
    /// linger is the opposite: it is what makes the close immediate.
    pub fn sever(self) {
        self.arm_reset();
        drop(self);
    }

    /// Arms the socket so that whenever it is dropped the close is an RST.
    ///
    /// For the callers that cannot consume the stream — a bound session holds it
    /// by `&mut` for its whole life — but still must not let it close politely.
    pub fn arm_reset(&self) {
        let _ = socket2::SockRef::from(self.socket()).set_linger(Some(std::time::Duration::ZERO));
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

    /// The option has to be on the socket, not merely requested: a keepalive that
    /// silently failed to apply leaves exactly the hang it was added to end, and
    /// nothing else in the proxy would ever say so.
    #[tokio::test]
    async fn arming_keepalive_sets_it_on_the_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connecting = tokio::spawn(async move { tokio::net::TcpStream::connect(address).await });
        let (accepted, _) = listener.accept().await.unwrap();
        let _client = connecting.await.unwrap().unwrap();

        let socket = socket2::SockRef::from(&accepted);
        assert!(
            !socket.keepalive().unwrap(),
            "a socket must not arrive with keepalive already on, or this proves nothing"
        );

        arm_keepalive(&accepted);
        assert!(socket.keepalive().unwrap());
        assert_eq!(socket.tcp_keepalive_time().unwrap(), KEEPALIVE_IDLE);
        assert_eq!(socket.tcp_keepalive_interval().unwrap(), KEEPALIVE_INTERVAL);
        assert_eq!(socket.tcp_keepalive_retries().unwrap(), KEEPALIVE_RETRIES);
    }
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
