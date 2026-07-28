use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::WireError;

/// A single tagged protocol frame, still undecoded.
///
/// The proxy forwards `DataRow` and `CopyData` at this level: the payload is
/// never inspected, only counted and relayed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub tag: u8,
    pub body: Bytes,
}

impl RawFrame {
    pub fn new(tag: u8, body: Bytes) -> Self {
        Self { tag, body }
    }

    /// Total number of bytes this frame occupies on the wire.
    pub fn wire_len(&self) -> usize {
        1 + 4 + self.body.len()
    }

    /// Writes the frame back out byte-for-byte, without decoding it.
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.reserve(self.wire_len());
        dst.put_u8(self.tag);
        dst.put_i32(i32::try_from(self.body.len() + 4).expect("frame body exceeds i32::MAX bytes"));
        dst.put_slice(&self.body);
    }

    pub(crate) fn reader(&self) -> Reader {
        Reader::new(self.body.clone())
    }
}

/// Bounds-checked cursor over a message body.
///
/// Every accessor returns [`WireError::Truncated`] rather than panicking, so a
/// hostile peer cannot crash the proxy with a short body.
pub(crate) struct Reader {
    buf: Bytes,
}

impl Reader {
    pub(crate) fn new(buf: Bytes) -> Self {
        Self { buf }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len()
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<Bytes, WireError> {
        if self.buf.len() < n {
            return Err(WireError::Truncated);
        }
        Ok(self.buf.split_to(n))
    }

    pub(crate) fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn i16(&mut self) -> Result<i16, WireError> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    pub(crate) fn i32(&mut self) -> Result<i32, WireError> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, WireError> {
        Ok(self.i32()?.cast_unsigned())
    }

    pub(crate) fn cstr(&mut self) -> Result<Bytes, WireError> {
        match memchr::memchr(0, &self.buf) {
            Some(end) => {
                let s = self.buf.split_to(end);
                self.buf.advance(1);
                Ok(s)
            }
            None => Err(WireError::UnterminatedString),
        }
    }

    /// Consumes everything left in the body.
    pub(crate) fn rest(&mut self) -> Bytes {
        self.buf.split_off(0)
    }

    /// Length-prefixed value: `-1` means SQL NULL, any other negative is invalid.
    pub(crate) fn nullable_value(&mut self) -> Result<Option<Bytes>, WireError> {
        let len = self.i32()?;
        if len == -1 {
            return Ok(None);
        }
        let len = usize::try_from(len).map_err(|_| WireError::InvalidLength(len))?;
        Ok(Some(self.take(len)?))
    }

    /// Rejects a declared element count that the remaining body cannot hold.
    ///
    /// Guards `Vec::with_capacity` against a peer that claims 32767 elements in
    /// a two-byte body.
    pub(crate) fn count(&self, declared: usize, min_bytes_each: usize) -> Result<usize, WireError> {
        if declared.saturating_mul(min_bytes_each) > self.remaining() {
            return Err(WireError::ImplausibleCount {
                count: declared,
                remaining: self.remaining(),
            });
        }
        Ok(declared)
    }

    pub(crate) fn end(self) -> Result<(), WireError> {
        if self.buf.is_empty() {
            Ok(())
        } else {
            Err(WireError::TrailingBytes(self.buf.len()))
        }
    }
}

pub(crate) fn put_cstr(dst: &mut BytesMut, s: &[u8]) {
    dst.put_slice(s);
    dst.put_u8(0);
}

pub(crate) fn put_nullable_value(dst: &mut BytesMut, value: Option<&Bytes>) {
    match value {
        None => dst.put_i32(-1),
        Some(v) => {
            dst.put_i32(i32::try_from(v.len()).expect("value exceeds i32::MAX bytes"));
            dst.put_slice(v);
        }
    }
}

pub(crate) fn put_count(dst: &mut BytesMut, n: usize) {
    dst.put_i16(i16::try_from(n).expect("element count exceeds i16::MAX"));
}

/// Writes a tagged frame, back-filling the length once the body is known.
pub(crate) fn frame(dst: &mut BytesMut, tag: u8, body: impl FnOnce(&mut BytesMut)) {
    dst.put_u8(tag);
    let len_at = dst.len();
    dst.put_i32(0);
    body(dst);
    let len = i32::try_from(dst.len() - len_at).expect("frame body exceeds i32::MAX bytes");
    dst[len_at..len_at + 4].copy_from_slice(&len.to_be_bytes());
}

/// Writes an untagged (pre-startup) frame, back-filling the self-inclusive length.
pub(crate) fn untagged_frame(dst: &mut BytesMut, body: impl FnOnce(&mut BytesMut)) {
    let len_at = dst.len();
    dst.put_i32(0);
    body(dst);
    let len = i32::try_from(dst.len() - len_at).expect("frame body exceeds i32::MAX bytes");
    dst[len_at..len_at + 4].copy_from_slice(&len.to_be_bytes());
}
