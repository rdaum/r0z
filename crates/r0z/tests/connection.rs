//! These are all tests using PUSH/PULL sockets created from a shared
//! context to connect two threads. As a compile-time test, this
//! creates one socket from a context, and passes this context to the
//! child thread, along with the endpoint address to connect to. The
//! second socket is the created in the child thread.

#[macro_use]
mod common;

#[cfg(unix)]
#[path = "unix/connection.rs"]
mod unix;

use std::str;
use std::thread;

test!(test_inproc, {
    with_connection(
        "inproc://pub",
        r0z::PUSH,
        send_message,
        r0z::PULL,
        check_recv,
    );
});

test!(test_tcp, {
    with_connection(
        "tcp://127.0.0.1:*",
        r0z::PUSH,
        send_message,
        r0z::PULL,
        check_recv,
    );
});

test!(test_poll_inproc, {
    with_connection(
        "inproc://pub",
        r0z::PUSH,
        send_message,
        r0z::PULL,
        check_poll,
    );
});

test!(test_poll_tcp, {
    with_connection(
        "tcp://127.0.0.1:*",
        r0z::PUSH,
        send_message,
        r0z::PULL,
        check_poll,
    );
});

fn send_message(_ctx: &r0z::Context, socket: &r0z::Socket) {
    socket.send("Message1", 0).unwrap();
}

fn check_poll(_ctx: &r0z::Context, pull_socket: &r0z::Socket) {
    {
        let mut poll_items = vec![pull_socket.as_poll_item(r0z::POLLIN)];
        assert_eq!(r0z::poll(&mut poll_items, 1000).unwrap(), 1);
        assert_eq!(poll_items[0].get_revents(), r0z::POLLIN);
    }

    let msg = pull_socket.recv_msg(r0z::DONTWAIT).unwrap();
    assert_eq!(&msg[..], b"Message1");
}

fn check_recv(_ctx: &r0z::Context, pull_socket: &r0z::Socket) {
    let msg = pull_socket.recv_msg(0).unwrap();
    assert_eq!(&msg[..], b"Message1");
}

//
// Utilities
//

pub fn with_connection<F, G>(
    address: &str,
    parent_type: r0z::SocketType,
    parent: F,
    child_type: r0z::SocketType,
    child: G,
) where
    F: for<'r> Fn(&'r r0z::Context, &r0z::Socket) + Send + 'static,
    G: for<'r> Fn(&'r r0z::Context, &r0z::Socket) + Send + 'static,
{
    let ctx = r0z::Context::new();

    let push_socket = ctx.socket(parent_type).unwrap();
    push_socket.bind(address).unwrap();
    let endpoint = push_socket.get_last_endpoint().unwrap().unwrap();

    let thread = {
        let w_ctx = ctx.clone();
        thread::spawn(move || {
            let pull_socket = connect_socket(&w_ctx, child_type, &endpoint).unwrap();
            child(&w_ctx, &pull_socket);
        })
    };

    parent(&ctx, &push_socket);

    thread.join().unwrap();
}

fn connect_socket(
    ctx: &r0z::Context,
    typ: r0z::SocketType,
    address: &str,
) -> Result<r0z::Socket, r0z::Error> {
    ctx.socket(typ)
        .and_then(|socket| socket.connect(address).map(|_| socket))
}
