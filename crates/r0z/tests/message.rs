#[macro_use]
mod common;

use quickcheck::{quickcheck, Arbitrary, Gen};
use r0z::Message;

// A pair which contains two non-equal values
#[derive(Clone, Debug)]
struct NePair<T>(T, T);

impl<T> Arbitrary for NePair<T>
where
    T: Eq + Arbitrary,
{
    fn arbitrary(g: &mut Gen) -> Self {
        let v1 = T::arbitrary(g);
        let v2 = (0..).map(|_| T::arbitrary(g)).find(|v| *v != v1).unwrap();
        NePair(v1, v2)
    }
}

quickcheck! {
    fn msg_cmp_eq(input: Vec<u8>) -> bool {
        Message::from(&input) == Message::from(&input)
    }

    fn msg_cmp_ne(input: NePair<Vec<u8>>) -> bool {
        Message::from(&input.0) != Message::from(&input.1)
    }

    fn msg_vec_roundtrip(input: Vec<u8>) -> bool {
        let original = Message::from(&input);
        Message::from(input) == original
    }
}

#[test]
fn owned_messages_preserve_contents_across_storage_sizes() {
    for size in [0, 1, 16, 31, 32, 33, 64, 4096] {
        let payload: Vec<u8> = (0..size).map(|index| index as u8).collect();
        let mut spare = Vec::with_capacity(size + 64);
        spare.extend_from_slice(&payload);
        let boxed = payload.clone().into_boxed_slice();
        let pointer = boxed.as_ptr();
        let mut message = Message::from(boxed);
        assert_eq!(&message[..], payload);
        if size > 32 {
            assert_eq!(message.as_ptr(), pointer);
        }
        let from_vec = Message::from(spare);
        assert_eq!(message, from_vec);
        if size > 0 {
            message[0] ^= 0xff;
            assert_eq!(from_vec[0], payload[0]);
        }
    }
}

#[test]
fn message_gets_rejects_interior_nul_property() {
    let mut msg = Message::new();
    assert_eq!(msg.gets("\0bad"), None);
}

#[test]
#[should_panic(expected = "message size too large")]
fn message_with_size_rejects_pathological_allocation_size() {
    let _ = Message::with_size(usize::MAX);
}

#[allow(deprecated)]
#[test]
#[should_panic(expected = "message size too large")]
fn message_with_capacity_unallocated_rejects_pathological_allocation_size() {
    unsafe {
        let _ = Message::with_capacity_unallocated(usize::MAX);
    }
}
