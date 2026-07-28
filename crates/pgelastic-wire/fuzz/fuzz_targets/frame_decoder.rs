#![no_main]

use libfuzzer_sys::fuzz_target;
use pgelastic_wire::{AuthState, BackendMessage, FrontendMessage, MessageBuffer};

fuzz_target!(|data: &[u8]| {
    let mut buf = MessageBuffer::new();
    buf.extend_from_slice(data);

    loop {
        let needed = buf.needed();
        match buf.next_frame() {
            Ok(Some(frame)) => {
                let _ = BackendMessage::decode(&frame);
                for auth in [
                    AuthState::Password,
                    AuthState::SaslInitial,
                    AuthState::SaslContinue,
                    AuthState::Gss,
                ] {
                    let _ = FrontendMessage::decode(&frame, auth);
                }
            }
            Ok(None) => {
                assert!(needed > 0, "no frame available yet but nothing more is needed");
                break;
            }
            Err(_) => break,
        }
    }
});
