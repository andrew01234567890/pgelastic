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

const READ_CHUNK: usize = 16 * 1024;

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
        if self.buf.capacity() - self.buf.len() < READ_CHUNK {
            self.buf.reserve(READ_CHUNK);
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

    pub fn next(&mut self) -> Result<Relayed, WireError> {
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
        let Relayed::Frame(frame) = relay.next().unwrap() else {
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
            assert_eq!(relay.next().unwrap(), Relayed::NeedMore);
        }
        relay.extend_from_slice(&bytes[bytes.len() - 1..]);
        let Relayed::Frame(frame) = relay.next().unwrap() else {
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
                match relay.next().unwrap() {
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
        let Relayed::Frame(frame) = relay.next().unwrap() else {
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
            match relay.next().unwrap() {
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
            relay.next(),
            Err(WireError::FrameTooLarge { max: 4096, .. })
        ));
    }

    #[test]
    fn a_length_below_four_is_rejected() {
        let mut relay = FrameRelay::default();
        relay.extend_from_slice(&[b'Q', 0xff, 0xff, 0xff, 0xff]);
        assert!(matches!(relay.next(), Err(WireError::InvalidLength(-1))));
    }
}
