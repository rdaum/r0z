// Test integration of zmq with a simple external event loop
//
// This excercises the `Socket::get_fd()` method in combination with
// `Socket::get_events()` to integrate with Unix `poll(2)` to check
// the basis for integration with external event loops works.

use log::debug;
use nix::poll::{self, PollFlags};
use std::os::fd::BorrowedFd;

use super::with_connection;

test!(test_external_poll_inproc, {
    with_connection(
        "inproc://test-poll",
        r0z::REQ,
        poll_client,
        r0z::REP,
        poll_worker,
    );
});

test!(test_external_poll_ipc, {
    with_connection(
        "ipc:///tmp/zmq-tokio-test",
        r0z::REQ,
        poll_client,
        r0z::REP,
        poll_worker,
    );
});

test!(test_external_poll_tcp, {
    with_connection(
        "tcp://127.0.0.1:*",
        r0z::REQ,
        poll_client,
        r0z::REP,
        poll_worker,
    );
});

fn poll_client(_ctx: &r0z::Context, socket: &r0z::Socket) {
    // TODO: we should use `poll::poll()` here as well.
    for i in 0..10 {
        let payload = format!("message {}", i);
        socket.send(&payload, 0).unwrap();
        let reply = socket.recv_msg(0).unwrap();
        assert_eq!(payload.as_bytes(), &reply[..]);
    }
    socket.send("", 0).unwrap();
    let last = socket.recv_msg(0).unwrap();
    assert_eq!(b"", &last[..]);
}

/// Keeps track of the polling state for the event signalling FD of a
/// single socket.
struct PollState<'a> {
    socket: &'a r0z::Socket,
    fds: [poll::PollFd<'a>; 1],
}

impl<'a> PollState<'a> {
    fn new(socket: &'a r0z::Socket) -> Self {
        let fd = socket.get_fd().unwrap();
        PollState {
            socket,
            fds: [poll::PollFd::new(
                // The returned raw FD is owned by `socket` and remains valid
                // for the lifetime of this `PollState`.
                unsafe { BorrowedFd::borrow_raw(fd) },
                PollFlags::POLLIN,
            )],
        }
    }

    /// Wait for one of `events` to happen.
    fn wait(&mut self, events: r0z::PollEvents) {
        while !(self.events().intersects(events)) {
            debug!("polling");
            let fds = &mut self.fds;
            poll::poll(fds, poll::PollTimeout::NONE).unwrap();
            debug!("poll done, events: {:?}", fds[0].revents());
            match fds[0].revents() {
                Some(events) => {
                    let events: PollFlags = events;
                    if !events.contains(PollFlags::POLLIN) {
                        continue;
                    }
                }
                _ => continue,
            }
        }
    }

    fn events(&self) -> r0z::PollEvents {
        self.socket.get_events().unwrap() as r0z::PollEvents
    }
}

fn poll_worker(_ctx: &r0z::Context, socket: &r0z::Socket) {
    let mut reply = None;
    let mut state = PollState::new(socket);
    loop {
        match reply.take() {
            None => {
                state.wait(r0z::POLLIN);
                let msg = socket.recv_msg(r0z::DONTWAIT).unwrap();
                reply = Some(msg);
            }
            Some(msg) => {
                state.wait(r0z::POLLOUT);
                let done = msg.is_empty();
                socket.send(msg, r0z::DONTWAIT).unwrap();
                if done {
                    break;
                }
            }
        }
    }
}
