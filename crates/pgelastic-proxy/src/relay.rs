//! Framing for the passthrough path.
//!
//! Distinct from [`MessageBuffer`](pgelastic_wire::MessageBuffer), which the
//! handshake uses: there, every message is small and has to be decoded. Here,
//! most bytes belong to `DataRow` and `CopyData` frames that must never be
//! decoded and must never be buffered whole either — a single row may legally
//! reach a gigabyte, so holding one would make per-connection memory a function
//! of peer input.
//!
//! Frames up to `inline_limit` are buffered and handed over as
//! [`Relayed::Frame`] so the caller can inspect their tag. Anything larger has
//! its header emitted immediately and its body streamed through in read-sized
//! chunks as [`Relayed::Opaque`], so the buffer never exceeds
//! `inline_limit + read_chunk` regardless of what the peer sends.

use bytes::{Buf, Bytes, BytesMut};
use pgelastic_wire::{RawFrame, WireError};

/// Largest frame buffered whole. Every message whose tag the proxy acts on
/// (`ReadyForQuery`, `Terminate`, the COPY control messages, `ErrorResponse`)
/// is orders of magnitude smaller than this.
pub const DEFAULT_INLINE_FRAME_BYTES: usize = 64 * 1024;

/// Ceiling on a declared frame length, matching the server's own 1 GiB limit.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1 << 30;

/// The largest read the relay will ask for, once a connection has shown it can fill one.
const MAX_READ_CHUNK: usize = 16 * 1024;

/// What a connection starts with, and what an idle one keeps.
///
/// Resident memory is touched pages, not reserved bytes, which is the whole reason this
/// constant matters. A 16 KiB buffer into which a client writes only a startup packet still
/// costs a full page; measured, dropping the starting request below the page size took
/// 2,790 bytes off every idle connection, because a small allocation shares a page with its
/// neighbours where a 16 KiB one does not.
///
/// 2 KiB rather than something smaller because it is also pgbouncer's `pkt_buf`, and a value
/// that cannot hold a typical query would make the growth path the common path.
const MIN_READ_CHUNK: usize = 2 * 1024;

/// One unit of relayable output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Relayed {
    /// A complete frame, small enough that its tag is worth looking at.
    Frame(RawFrame),
    /// Bytes of an oversized frame, already framed on the wire.
    Opaque(Bytes),
    /// Nothing more can be produced without another read.
    NeedMore,
}

#[derive(Debug)]
pub struct FrameRelay {
    buf: BytesMut,
    inline_limit: usize,
    max_frame_len: usize,
    streaming: usize,
    /// Grows towards [`MAX_READ_CHUNK`] as reads come back full, and never shrinks.
    ///
    /// One-way on purpose. A connection relaying a result set wants large reads and would pay
    /// a syscall for every shrink-and-regrow cycle; a connection that sits idle never grows in
    /// the first place, which is the case this exists to make cheap. The distinction the
    /// benchmark cares about -- thousands of idle clients against a hundred busy backends --
    /// falls out of the traffic rather than having to be configured.
    read_chunk: usize,
}

impl Default for FrameRelay {
    fn default() -> Self {
        Self::new(DEFAULT_INLINE_FRAME_BYTES, DEFAULT_MAX_FRAME_BYTES)
    }
}

impl FrameRelay {
    pub fn new(inline_limit: usize, max_frame_len: usize) -> Self {
        Self {
            buf: BytesMut::new(),
            inline_limit: inline_limit.max(1),
            max_frame_len,
            streaming: 0,
            read_chunk: MIN_READ_CHUNK,
        }
    }

    /// Seeds the relay with bytes the handshake read but did not consume.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// The buffer to read socket bytes into.
    ///
    /// Cancel-safe in the same sense as `MessageBuffer::read_target`: the
    /// partially-read frame lives here, not inside the read future.
    pub fn read_target(&mut self) -> &mut BytesMut {
        // A buffer still holding a chunk's worth of unparsed bytes is one whose last read came
        // back full, so the peer has more to say than the current size asks for. Doubling here
        // rather than on a byte count keeps a connection that sends one large message from
        // being treated the same as one that streams.
        if self.buf.len() >= self.read_chunk && self.read_chunk < MAX_READ_CHUNK {
            self.read_chunk = (self.read_chunk * 2).min(MAX_READ_CHUNK);
        }
        if self.buf.capacity() - self.buf.len() < self.read_chunk {
            self.buf.reserve(self.read_chunk);
        }
        &mut self.buf
    }

    /// True when no partial frame is held, so the connection may be closed
    /// without truncating a message.
    pub fn at_frame_boundary(&self) -> bool {
        self.buf.is_empty() && self.streaming == 0
    }

    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// The next unit of output, or [`Relayed::NeedMore`].
    ///
    /// Not `Iterator`: `NeedMore` is a legitimate non-terminal state, so an
    /// iterator would have to conflate "nothing yet" with "nothing ever".
    pub fn next_output(&mut self) -> Result<Relayed, WireError> {
        if self.streaming > 0 {
            if self.buf.is_empty() {
                return Ok(Relayed::NeedMore);
            }
            let take = self.streaming.min(self.buf.len());
            self.streaming -= take;
            return Ok(Relayed::Opaque(self.buf.split_to(take).freeze()));
        }

        if self.buf.len() < 5 {
            return Ok(Relayed::NeedMore);
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

        if body_len > self.inline_limit {
            self.streaming = body_len;
            return Ok(Relayed::Opaque(self.buf.split_to(5).freeze()));
        }

        if self.buf.len() < body_len + 5 {
            return Ok(Relayed::NeedMore);
        }
        let tag = self.buf[0];
        self.buf.advance(5);
        let body = self.buf.split_to(body_len).freeze();
        Ok(Relayed::Frame(RawFrame { tag, body }))
    }
}

#[cfg(test)]
mod tests {
    /// A frame the relay will accept: one tag byte, a big-endian length that counts itself,
    /// and a body. A run of zero bytes is not a frame -- it decodes to a negative body length
    /// -- and a loop that drains until `NeedMore` never terminates on the error that produces.
    fn frame(body: usize) -> Vec<u8> {
        let mut out = vec![b'D'];
        out.extend_from_slice(
            &i32::try_from(body + 4)
                .expect("a test frame fits in i32")
                .to_be_bytes(),
        );
        out.extend(std::iter::repeat_n(b'x', body));
        out
    }

    /// Drains what the relay can produce, stopping on either terminal answer.
    fn drain(relay: &mut FrameRelay) {
        while matches!(
            relay.next_output(),
            Ok(Relayed::Frame(_) | Relayed::Opaque(_))
        ) {}
    }

    /// An idle connection never grows its read buffer, which is the entire point: resident
    /// memory is touched pages, and thousands of connections that each touch a page they do
    /// not need is what put the proxy well above pgbouncer per idle connection.
    #[test]
    fn a_connection_that_never_fills_a_read_keeps_the_small_buffer() {
        let mut relay = FrameRelay::default();

        for _ in 0..50 {
            relay.read_target().extend_from_slice(&frame(64));
            drain(&mut relay);
        }

        assert_eq!(
            relay.read_chunk, MIN_READ_CHUNK,
            "an idle connection grew its buffer without ever filling one"
        );
    }

    /// A connection that does fill its reads has to reach the large chunk, or a result set
    /// would be relayed a couple of kilobytes per syscall.
    #[test]
    fn a_connection_that_fills_its_reads_grows_to_the_large_chunk() {
        let mut relay = FrameRelay::default();

        for _ in 0..16 {
            let chunk = relay.read_chunk;
            relay.read_target().extend_from_slice(&frame(chunk));
        }

        assert_eq!(
            relay.read_chunk, MAX_READ_CHUNK,
            "a relay whose reads keep coming back full must reach the large chunk"
        );
    }

    /// Growth is one-way. A shrink would cost a reallocation every time a busy connection went
    /// briefly quiet, which is the common rhythm of a transaction-pooled link.
    #[test]
    fn the_read_chunk_does_not_shrink_once_grown() {
        let mut relay = FrameRelay::default();

        for _ in 0..16 {
            let chunk = relay.read_chunk;
            relay.read_target().extend_from_slice(&frame(chunk));
        }
        drain(&mut relay);
        let _ = relay.read_target();

        assert_eq!(relay.read_chunk, MAX_READ_CHUNK);
    }

    use super::*;

    fn wire(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![tag];
        v.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
        v.extend_from_slice(body);
        v
    }

    #[test]
    fn a_small_frame_arrives_whole_and_tagged() {
        let mut relay = FrameRelay::default();
        relay.extend_from_slice(&wire(b'Z', b"I"));
        let Relayed::Frame(frame) = relay.next_output().unwrap() else {
            panic!("expected a whole frame");
        };
        assert_eq!(frame.tag, b'Z');
        assert_eq!(frame.body.as_ref(), b"I");
        assert!(relay.at_frame_boundary());
    }

    #[test]
    fn a_frame_split_across_reads_is_reassembled() {
        let bytes = wire(b'Q', b"select 1\0");
        let mut relay = FrameRelay::default();
        for byte in &bytes[..bytes.len() - 1] {
            relay.extend_from_slice(&[*byte]);
            assert_eq!(relay.next_output().unwrap(), Relayed::NeedMore);
        }
        relay.extend_from_slice(&bytes[bytes.len() - 1..]);
        let Relayed::Frame(frame) = relay.next_output().unwrap() else {
            panic!("expected a whole frame");
        };
        assert_eq!(frame.tag, b'Q');
    }

    #[test]
    fn an_oversized_frame_streams_without_being_buffered() {
        let body = vec![7u8; 40_000];
        let bytes = wire(b'd', &body);
        let mut relay = FrameRelay::new(1024, DEFAULT_MAX_FRAME_BYTES);

        let mut out = Vec::new();
        let mut peak = 0;
        for chunk in bytes.chunks(4096) {
            relay.extend_from_slice(chunk);
            peak = peak.max(relay.buffered());
            loop {
                match relay.next_output().unwrap() {
                    Relayed::Opaque(b) => out.extend_from_slice(&b),
                    Relayed::NeedMore => break,
                    Relayed::Frame(_) => panic!("an oversized frame must not be buffered whole"),
                }
            }
        }
        assert_eq!(out, bytes);
        assert!(peak <= 4096, "peak buffer was {peak} bytes");
        assert!(relay.at_frame_boundary());
    }

    #[test]
    fn a_frame_exactly_at_the_inline_limit_is_still_buffered_whole() {
        let body = vec![1u8; 1024];
        let mut relay = FrameRelay::new(1024, DEFAULT_MAX_FRAME_BYTES);
        relay.extend_from_slice(&wire(b'D', &body));
        let Relayed::Frame(frame) = relay.next_output().unwrap() else {
            panic!("expected a whole frame");
        };
        assert_eq!(frame.body.len(), 1024);
    }

    #[test]
    fn frames_after_a_streamed_body_are_still_framed() {
        let mut bytes = wire(b'd', &vec![3u8; 5000]);
        bytes.extend_from_slice(&wire(b'c', b""));
        bytes.extend_from_slice(&wire(b'Z', b"I"));

        let mut relay = FrameRelay::new(256, DEFAULT_MAX_FRAME_BYTES);
        relay.extend_from_slice(&bytes);

        let mut tags = Vec::new();
        let mut opaque = 0usize;
        loop {
            match relay.next_output().unwrap() {
                Relayed::Frame(f) => tags.push(f.tag),
                Relayed::Opaque(b) => opaque += b.len(),
                Relayed::NeedMore => break,
            }
        }
        assert_eq!(opaque, 5005);
        assert_eq!(tags, vec![b'c', b'Z']);
    }

    #[test]
    fn a_declared_length_over_the_ceiling_is_rejected_before_allocating() {
        let mut relay = FrameRelay::new(1024, 4096);
        relay.extend_from_slice(&[b'd', 0x3f, 0xff, 0xff, 0xff]);
        assert!(matches!(
            relay.next_output(),
            Err(WireError::FrameTooLarge { max: 4096, .. })
        ));
    }

    #[test]
    fn a_length_below_four_is_rejected() {
        let mut relay = FrameRelay::default();
        relay.extend_from_slice(&[b'Q', 0xff, 0xff, 0xff, 0xff]);
        assert!(matches!(
            relay.next_output(),
            Err(WireError::InvalidLength(-1))
        ));
    }
}
