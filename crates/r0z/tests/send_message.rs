#[macro_use]
mod common;

use r0z::{Context, Error, Message, Socket, DONTWAIT, SNDMORE};

fn assert_retained(message: &Message, pointer: *const u8, payload: &[u8]) {
    assert_eq!(message.as_ptr(), pointer);
    assert_eq!(&message[..], payload);
}

fn receive(socket: &Socket, payload: &[u8], pointer: *const u8, more: bool) {
    let received = socket.recv_msg(0).unwrap();
    assert_eq!(&received[..], payload);
    assert_eq!(received.get_more(), more);
    // Large inproc frames transfer their storage. Small frames can use native inline storage.
    if payload.len() >= 4096 {
        assert_eq!(received.as_ptr(), pointer);
    }
}

test!(eagain_retains_message_for_retry, {
    for size in [0, 16, 4096, 1024 * 1024] {
        let context = Context::new();
        let sender = context.socket(r0z::PUSH).unwrap();
        sender.set_linger(0).unwrap();
        sender.set_sndtimeo(2000).unwrap();
        sender.bind("inproc://retry-message").unwrap();
        let payload = vec![0x5a; size];
        let mut message = Message::from(payload.as_slice());
        let pointer = message.as_ptr();
        for _ in 0..3 {
            assert_eq!(
                sender.send_message(&mut message, DONTWAIT),
                Err(Error::EAGAIN)
            );
            assert_retained(&message, pointer, &payload);
        }
        let receiver = context.socket(r0z::PULL).unwrap();
        receiver.set_rcvtimeo(2000).unwrap();
        receiver.connect("inproc://retry-message").unwrap();
        sender.send_message(&mut message, 0).unwrap();
        assert!(message.is_empty());
        receive(&receiver, &payload, pointer, false);
        assert_eq!(receiver.recv_msg(DONTWAIT).unwrap_err(), Error::EAGAIN);
    }
});

test!(multipart_frames_transfer_in_order, {
    let context = Context::new();
    let sender = context.socket(r0z::PUSH).unwrap();
    sender.set_linger(0).unwrap();
    sender.set_sndtimeo(2000).unwrap();
    sender.bind("inproc://multipart-message").unwrap();
    let receiver = context.socket(r0z::PULL).unwrap();
    receiver.set_rcvtimeo(2000).unwrap();
    receiver.connect("inproc://multipart-message").unwrap();
    let payloads = [vec![1; 4096], vec![], vec![2; 16], vec![3; 8192]];
    let mut pointers = Vec::new();
    for (index, payload) in payloads.iter().enumerate() {
        let mut message = Message::from(payload.as_slice());
        pointers.push(message.as_ptr());
        let flags = if index + 1 == payloads.len() {
            0
        } else {
            SNDMORE
        };
        sender.send_message(&mut message, flags).unwrap();
        assert!(message.is_empty());
    }
    for (index, payload) in payloads.iter().enumerate() {
        receive(
            &receiver,
            payload,
            pointers[index],
            index + 1 != payloads.len(),
        );
    }
});

test!(native_errors_retain_message_for_drop_or_another_socket, {
    let context = Context::new();
    let payload = vec![0x42; 4096];
    let mut message = Message::from(payload.as_slice());
    let pointer = message.as_ptr();
    for (kind, error) in [(r0z::PULL, Error::ENOTSUP), (r0z::REP, Error::EFSM)] {
        let socket = context.socket(kind).unwrap();
        assert_eq!(socket.send_message(&mut message, DONTWAIT), Err(error));
        assert_retained(&message, pointer, &payload);
    }
    let sender = context.socket(r0z::PUSH).unwrap();
    sender.set_linger(0).unwrap();
    sender.set_sndtimeo(2000).unwrap();
    sender.bind("inproc://after-error").unwrap();
    let receiver = context.socket(r0z::PULL).unwrap();
    receiver.set_rcvtimeo(2000).unwrap();
    receiver.connect("inproc://after-error").unwrap();
    sender.send_message(&mut message, 0).unwrap();
    receive(&receiver, &payload, pointer, false);

    let mut failed = Message::from(payload.as_slice());
    let pointer = failed.as_ptr();
    let mut terminating = context.clone();
    let terminate = std::thread::spawn(move || terminating.destroy());
    // Wait for termination without consuming another message from the native queue.
    assert_eq!(receiver.recv_msg(0).unwrap_err(), Error::ETERM);
    // Each socket receives its own stop command. The receiver can stop before this one does.
    loop {
        match sender.get_events() {
            Err(Error::ETERM) => break,
            Ok(_) => std::thread::yield_now(),
            Err(error) => panic!("unexpected termination error: {}", error),
        }
    }
    assert_eq!(
        sender.send_message(&mut failed, DONTWAIT),
        Err(Error::ETERM)
    );
    assert_retained(&failed, pointer, &payload);
    drop(failed);
    drop(sender);
    drop(receiver);
    terminate.join().unwrap().unwrap();
});
