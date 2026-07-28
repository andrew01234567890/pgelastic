//! Cancel-safe framing buffer.
//!
//! Deliberately hand-rolled on [`BytesMut`] rather than built on
//! `tokio_util::codec::Framed`: that decoder re-initialises buffer memory when
//! a frame is large (tokio-rs/tokio#3417), which is exactly the `CopyData` case
//! this proxy spends most of its bytes in.
//!
//! # Cancel safety
//!
//! [`MessageBuffer`] owns the partially-read frame, and no method on it is
//! `async`. A read loop looks like:
//!
//! ```ignore
//! loop {
//!     if let Some(frame) = buf.next_frame()? {
//!         return Ok(frame);
//!     }
//!     socket.read_buf(buf.read_target()).await?; // the only await point
//! }
//! ```
//!
//! If the future holding that loop is dropped between polls, the bytes already
//! accumulated stay in the caller-owned `MessageBuffer`; resuming later with a
//! fresh loop sees exactly the same state. Nothing is buffered inside the
//! future, so there is no partial frame to lose.

use bytes::{Buf, BytesMut};

use crate::codec::RawFrame;
use crate::error::WireError;
use crate::startup::MAX_STARTUP_PACKET_LENGTH;

/// Default ceiling on a single decoded frame, matching the server's own 1 GiB
/// limit on message size.
pub const DEFAULT_MAX_FRAME_LEN: usize = 1 << 30;

/// Default upper bound on how much spare capacity a single [`read_target`]
/// call will reserve.
///
/// A frame header is five bytes of peer-controlled input; honouring its
/// declared length as an allocation would let one packet ask for a gigabyte.
/// Growth is amortised over several reads instead.
///
/// [`read_target`]: MessageBuffer::read_target
pub const DEFAULT_READ_CHUNK: usize = 16 * 1024;

#[derive(Debug)]
pub struct MessageBuffer {
    buf: BytesMut,
    max_frame_len: usize,
    read_chunk: usize,
}

impl Default for MessageBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageBuffer {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::new(),
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            read_chunk: DEFAULT_READ_CHUNK,
        }
    }

    #[must_use]
    pub fn with_max_frame_len(mut self, max: usize) -> Self {
        self.max_frame_len = max;
        self
    }

    #[must_use]
    pub fn with_read_chunk(mut self, chunk: usize) -> Self {
        self.read_chunk = chunk.max(5);
        self
    }

    pub fn max_frame_len(&self) -> usize {
        self.max_frame_len
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// The first unread byte, without consuming it.
    ///
    /// The pre-startup machine needs this to spot a raw TLS `ClientHello`
    /// (`0x16`), which carries no length prefix of its own.
    pub fn peek(&self) -> Option<u8> {
        self.buf.first().copied()
    }

    /// Appends already-read bytes.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// The buffer to read socket bytes into.
    ///
    /// Reserves [`needed`] bytes ahead of the read, but never more than the
    /// configured read chunk: the declared length is peer-controlled and must
    /// not become an allocation on its own. A caller that wants a frame of up
    /// to `n` bytes to arrive in a single read pairs
    /// [`with_read_chunk(n)`][MessageBuffer::with_read_chunk] with
    /// [`with_max_frame_len(n)`][MessageBuffer::with_max_frame_len], so the
    /// declared length is rejected before it is ever trusted.
    ///
    /// [`needed`]: MessageBuffer::needed
    pub fn read_target(&mut self) -> &mut BytesMut {
        let want = self.needed().min(self.read_chunk).max(1);
        if self.buf.capacity() - self.buf.len() < want {
            self.buf.reserve(want);
        }
        &mut self.buf
    }

    /// Discards every buffered byte.
    ///
    /// Mandatory before handing the socket to the TLS layer after answering an
    /// `SSLRequest`: anything the client pipelined behind that request arrived
    /// in the clear and must not be treated as if it came from inside the
    /// tunnel (CVE-2021-23214 / CVE-2021-23222).
    pub fn discard_all(&mut self) {
        self.buf.clear();
    }

    /// How many more bytes are needed before [`next_frame`] can answer.
    ///
    /// Returns `0` when a frame is already available *and* when the buffered
    /// header is malformed — in both cases the caller should call
    /// [`next_frame`] rather than wait for more input.
    ///
    /// [`next_frame`]: MessageBuffer::next_frame
    pub fn needed(&self) -> usize {
        match self.frame_len() {
            Ok(Some(total)) => total.saturating_sub(self.buf.len()),
            Ok(None) => 5 - self.buf.len(),
            Err(()) => 0,
        }
    }

    /// `Ok(Some(total_wire_len))`, `Ok(None)` if the header is incomplete, or
    /// `Err(())` if the header is present but invalid.
    fn frame_len(&self) -> Result<Option<usize>, ()> {
        if self.buf.len() < 5 {
            return Ok(None);
        }
        let declared = i32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]);
        let Ok(declared) = usize::try_from(declared) else {
            return Err(());
        };
        if declared < 4 {
            return Err(());
        }
        let body = declared - 4;
        if body > self.max_frame_len {
            return Err(());
        }
        Ok(Some(body + 5))
    }

    /// Splits off the next complete tagged frame, if one is fully buffered.
    ///
    /// The buffer is only advanced once the whole frame is present, so calling
    /// this on a partial frame is free of side effects.
    pub fn next_frame(&mut self) -> Result<Option<RawFrame>, WireError> {
        if self.buf.len() < 5 {
            return Ok(None);
        }
        let declared = i32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]);
        let body_len = usize::try_from(declared)
            .ok()
            .and_then(|d| d.checked_sub(4))
            .ok_or(WireError::InvalidLength(declared))?;
        if body_len > self.max_frame_len {
            return Err(WireError::FrameTooLarge {
                len: body_len,
                max: self.max_frame_len,
            });
        }
        if self.buf.len() < body_len + 5 {
            return Ok(None);
        }
        let tag = self.buf[0];
        self.buf.advance(5);
        let body = self.buf.split_to(body_len).freeze();
        Ok(Some(RawFrame { tag, body }))
    }

    /// How many more bytes are needed before [`next_startup_frame`] can answer.
    ///
    /// [`next_startup_frame`]: MessageBuffer::next_startup_frame
    pub fn needed_startup(&self) -> usize {
        if self.buf.len() < 4 {
            return 4 - self.buf.len();
        }
        let declared = i32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
        let Ok(total) = usize::try_from(declared) else {
            return 0;
        };
        if !(8..=MAX_STARTUP_PACKET_LENGTH).contains(&total) {
            return 0;
        }
        total.saturating_sub(self.buf.len())
    }

    /// Splits off the next pre-startup packet body, excluding its length prefix.
    ///
    /// Pre-startup packets carry no type byte, and their Int32 length is
    /// self-inclusive. Anything larger than [`MAX_STARTUP_PACKET_LENGTH`] is
    /// rejected without allocating.
    pub fn next_startup_frame(&mut self) -> Result<Option<bytes::Bytes>, WireError> {
        if self.buf.len() < 4 {
            return Ok(None);
        }
        let declared = i32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
        let total = usize::try_from(declared).map_err(|_| WireError::InvalidLength(declared))?;
        if total < 8 {
            return Err(WireError::InvalidLength(declared));
        }
        if total > MAX_STARTUP_PACKET_LENGTH {
            return Err(WireError::StartupPacketTooLarge { len: total });
        }
        if self.buf.len() < total {
            return Ok(None);
        }
        self.buf.advance(4);
        Ok(Some(self.buf.split_to(total - 4).freeze()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn frame_bytes(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![tag];
        v.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
        v.extend_from_slice(body);
        v
    }

    #[test]
    fn whole_frame_decodes_in_one_go() {
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&frame_bytes(b'Q', b"select 1\0"));
        let frame = buf.next_frame().unwrap().unwrap();
        assert_eq!(frame.tag, b'Q');
        assert_eq!(frame.body, Bytes::from_static(b"select 1\0"));
        assert!(buf.is_empty());
    }

    #[test]
    fn byte_at_a_time_matches_whole_frame() {
        let wire = frame_bytes(b'Q', b"select 1\0");

        let mut whole = MessageBuffer::new();
        whole.extend_from_slice(&wire);
        let expected = whole.next_frame().unwrap().unwrap();

        let mut drip = MessageBuffer::new();
        for (i, byte) in wire.iter().enumerate() {
            assert!(drip.next_frame().unwrap().is_none());
            let needed = drip.needed();
            assert!(needed > 0 && needed <= wire.len() - i);
            if i >= 5 {
                assert_eq!(needed, wire.len() - i);
            }
            drip.extend_from_slice(&[*byte]);
        }
        assert_eq!(drip.needed(), 0);
        assert_eq!(drip.next_frame().unwrap().unwrap(), expected);
    }

    #[test]
    fn needed_counts_down_to_zero() {
        let mut buf = MessageBuffer::new();
        assert_eq!(buf.needed(), 5);
        buf.extend_from_slice(&[b'D', 0, 0, 0, 104]);
        assert_eq!(buf.needed(), 100);
        buf.extend_from_slice(&[0u8; 100]);
        assert_eq!(buf.needed(), 0);
    }

    #[test]
    fn a_read_future_dropped_mid_poll_loses_nothing() {
        use std::future::{Future, pending};
        use std::pin::pin;
        use std::task::{Context, Poll, Waker};

        let wire = frame_bytes(b'd', &[7u8; 400]);
        let mut buf = MessageBuffer::new();
        let mut cx = Context::from_waker(Waker::noop());

        for chunk in wire.chunks(37) {
            let target = buf.read_target();
            let mut read = pin!(async {
                target.extend_from_slice(chunk);
                pending::<()>().await;
            });
            assert!(matches!(read.as_mut().poll(&mut cx), Poll::Pending));
        }

        let frame = buf.next_frame().unwrap().unwrap();
        assert_eq!(frame.body.len(), 400);
        assert!(buf.is_empty());
    }

    #[test]
    fn a_lying_length_neither_panics_nor_allocates() {
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&[b'd', 0x3f, 0xff, 0xff, 0xff]);
        assert!(buf.next_frame().unwrap().is_none());

        let before = buf.read_target().capacity();
        let after = buf.read_target().capacity();
        assert_eq!(before, after);
        assert!(
            after < 1024 * 1024,
            "reserved {after} bytes on a 5-byte lie"
        );
    }

    #[test]
    fn a_matched_chunk_and_frame_limit_gives_a_single_read() {
        let wire = frame_bytes(b'd', &[1u8; 8_000]);
        let mut buf = MessageBuffer::new()
            .with_max_frame_len(16_384)
            .with_read_chunk(16_384);

        buf.extend_from_slice(&wire[..5]);
        assert_eq!(buf.needed(), 8_000);

        let target = buf.read_target();
        assert!(target.capacity() - target.len() >= 8_000);

        buf.extend_from_slice(&wire[5..]);
        assert_eq!(buf.next_frame().unwrap().unwrap().body.len(), 8_000);
    }

    #[test]
    fn frame_over_the_limit_is_rejected() {
        let mut buf = MessageBuffer::new().with_max_frame_len(64);
        buf.extend_from_slice(&[b'd', 0, 0, 1, 0]);
        assert!(matches!(
            buf.next_frame(),
            Err(WireError::FrameTooLarge { len: 252, max: 64 })
        ));
    }

    #[test]
    fn length_below_four_is_rejected() {
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&[b'Q', 0xff, 0xff, 0xff, 0xff]);
        assert!(matches!(
            buf.next_frame(),
            Err(WireError::InvalidLength(-1))
        ));
    }

    #[test]
    fn empty_body_frame_round_trips() {
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&frame_bytes(b'S', b""));
        let frame = buf.next_frame().unwrap().unwrap();
        assert_eq!(frame.tag, b'S');
        assert!(frame.body.is_empty());
    }

    #[test]
    fn two_frames_in_one_read() {
        let mut buf = MessageBuffer::new();
        let mut wire = frame_bytes(b'P', b"\0\0\0\0");
        wire.extend_from_slice(&frame_bytes(b'S', b""));
        buf.extend_from_slice(&wire);
        assert_eq!(buf.next_frame().unwrap().unwrap().tag, b'P');
        assert_eq!(buf.next_frame().unwrap().unwrap().tag, b'S');
        assert!(buf.next_frame().unwrap().is_none());
    }

    #[test]
    fn startup_packet_over_the_limit_is_rejected() {
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&10_001i32.to_be_bytes());
        assert!(matches!(
            buf.next_startup_frame(),
            Err(WireError::StartupPacketTooLarge { len: 10_001 })
        ));
        assert_eq!(buf.needed_startup(), 0);
    }

    #[test]
    fn startup_packet_at_the_limit_is_accepted() {
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(&10_000i32.to_be_bytes());
        assert_eq!(buf.needed_startup(), 9_996);
        buf.extend_from_slice(&vec![0u8; 9_996]);
        assert_eq!(buf.next_startup_frame().unwrap().unwrap().len(), 9_996);
    }

    #[test]
    fn discard_all_drops_pipelined_plaintext() {
        let mut buf = MessageBuffer::new();
        buf.extend_from_slice(b"\x00\x00\x00\x08\x04\xd2\x16\x2fsneaky");
        let _ = buf.next_startup_frame().unwrap().unwrap();
        assert!(!buf.is_empty());
        buf.discard_all();
        assert!(buf.is_empty());
    }
}
