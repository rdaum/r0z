#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(property) = std::str::from_utf8(data) {
        let mut msg = r0z::Message::new();
        let _ = msg.gets(property);
    }
});
