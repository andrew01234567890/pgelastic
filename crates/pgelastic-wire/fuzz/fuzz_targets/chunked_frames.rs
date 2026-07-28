#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use pgelastic_wire::MessageBuffer;

/// A byte stream split the way a socket would split it, so the framing is
/// exercised across read boundaries rather than on one contiguous slice.
#[derive(Debug, Arbitrary)]
struct ChunkedStream<'a> {
    chunks: Vec<&'a [u8]>,
}

fuzz_target!(|stream: ChunkedStream<'_>| {
    let mut whole = Vec::new();
    let mut chunked = MessageBuffer::new();
    let mut chunked_frames = Vec::new();

    for chunk in &stream.chunks {
        whole.extend_from_slice(chunk);
        chunked.extend_from_slice(chunk);
        while let Ok(Some(frame)) = chunked.next_frame() {
            chunked_frames.push(frame);
        }
    }

    let mut at_once = MessageBuffer::new();
    at_once.extend_from_slice(&whole);
    let mut at_once_frames = Vec::new();
    while let Ok(Some(frame)) = at_once.next_frame() {
        at_once_frames.push(frame);
    }

    assert_eq!(chunked_frames, at_once_frames);
});
