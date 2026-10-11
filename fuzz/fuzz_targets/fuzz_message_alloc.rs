#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Exercise owned storage on both sides of the small-message threshold.
    let from_vec = r0z::Message::from(data.to_vec());
    let from_box = r0z::Message::from(data.to_vec().into_boxed_slice());
    assert_eq!(&from_vec[..], data);
    assert_eq!(from_vec, from_box);

    if data.len() < std::mem::size_of::<u64>() {
        return;
    }

    let mut len_bytes = [0u8; 8];
    len_bytes.copy_from_slice(&data[..8]);

    // Keep the fuzzer in a useful range while still exercising allocator and
    // size-boundary paths in Message constructors.
    let len = (u64::from_le_bytes(len_bytes) as usize) & 0x00FF_FFFF;

    let _ = r0z::Message::with_size(len);

    #[allow(deprecated)]
    unsafe {
        let _ = r0z::Message::with_capacity_unallocated(len);
    }
});
