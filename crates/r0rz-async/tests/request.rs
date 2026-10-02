#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use r0rz::{Context, SocketType};

use r0rz_async::{Multipart, Result};
use utils::{generate_tcp_address, msg, sync_echo};

mod utils;

#[test]
fn single_message() -> Result<()> {
    utils::run(|| async {
        let address = generate_tcp_address();
        let ctx = Context::new();
        let requester = utils::request(&ctx).connect(&address)?;

        let echo = sync_echo(address, SocketType::REP, 1);

        let m1 = "Msg";
        let m2 = "Msg (contd.)";
        let message = Multipart::from(vec![msg(m1.as_bytes()), msg(m2.as_bytes())]);
        let reply_receiver = requester.send(message).await?;
        if let Ok((multipart, _)) = reply_receiver.recv().await {
            let expected: Multipart = vec![msg(m1.as_bytes()), msg(m2.as_bytes())].into();
            assert_eq!(expected, multipart);
        } else {
            panic!("Reply is missing.");
        }

        echo.join().unwrap();

        Ok(())
    })
}

#[test]
fn request_hammer() -> Result<()> {
    utils::run(|| async {
        let address = generate_tcp_address();
        let ctx = Context::new();
        let mut req_sock = utils::request(&ctx).connect(&address)?;

        let count = 1_000;
        let echo = sync_echo(address, SocketType::REP, count);

        for i in 0..count {
            let m1 = format!("Msg #{}", i);
            let m2 = format!("Msg #{} (contd.)", i);
            let message = Multipart::from(vec![msg(m1.as_bytes()), msg(m2.as_bytes())]);
            let reply_sock = req_sock.send(message).await?;
            let (multipart, req) = reply_sock.recv().await?;
            req_sock = req;
            let expected: Multipart = vec![msg(m1.as_bytes()), msg(m2.as_bytes())].into();
            assert_eq!(expected, multipart);
        }

        echo.join().unwrap();

        Ok(())
    })
}
