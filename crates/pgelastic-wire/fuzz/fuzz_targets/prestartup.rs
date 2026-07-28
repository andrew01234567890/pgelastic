#![no_main]

use libfuzzer_sys::fuzz_target;
use pgelastic_wire::{MessageBuffer, PreStartup, PreStartupMachine};

fuzz_target!(|data: &[u8]| {
    let mut buf = MessageBuffer::new();
    buf.extend_from_slice(data);
    let mut machine = PreStartupMachine::new();

    loop {
        match machine.step(&mut buf) {
            Ok(Some(PreStartup::DirectTls)) => break,
            Ok(Some(PreStartup::Startup(startup))) => {
                let _ = startup.nested_options();
                let _ = startup.extension_parameters().count();
                break;
            }
            Ok(Some(PreStartup::CancelRequest(cancel))) => {
                assert!((4..=256).contains(&cancel.key.len()));
                break;
            }
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break,
        }
    }
});
