#![cfg(all(unix, any(feature = "tokio", feature = "async-io", feature = "compio")))]

use r0z::{Context, SocketType};

use r0z_async::{Multipart, Result};
use utils::{generate_tcp_address, msg, sync_echo};

mod utils;

#[test]
fn single_message() -> Result<()> {
    utils::run(|| async {
        let address = generate_tcp_address();
        let progress = utils::Progress::new("requester", 1);
        progress.endpoint(&address);
        let ctx = Context::new();
        progress.phase("connecting");
        let mut requester = utils::request(&ctx).connect(&address)?;

        let echo = sync_echo(address, SocketType::REP, 1);

        let m1 = "Msg";
        let m2 = "Msg (contd.)";
        let message = Multipart::from(vec![msg(m1.as_bytes()), msg(m2.as_bytes())]);
        progress.phase("sending request");
        requester.send(message).await?;
        progress.phase("receiving reply");
        match requester.recv().await {
            Ok(multipart) => {
                let expected: Multipart = vec![msg(m1.as_bytes()), msg(m2.as_bytes())].into();
                assert_eq!(expected, multipart);
            }
            _ => {
                panic!("Reply is missing.");
            }
        }

        progress.advance(1);
        progress.phase("joining echo");
        echo.join().unwrap();
        progress.phase("dropping socket");
        drop(requester);
        progress.phase("dropping context");
        drop(ctx);
        progress.phase("done");

        Ok(())
    })
}

#[test]
fn request_hammer() -> Result<()> {
    utils::run(|| async {
        let address = generate_tcp_address();
        let count = 1_000;
        let progress = utils::Progress::new("requester", count);
        progress.endpoint(&address);
        let ctx = Context::new();
        progress.phase("connecting");
        let mut req_sock = utils::request(&ctx).connect(&address)?;

        let echo = sync_echo(address, SocketType::REP, count);

        for i in 0..count {
            let m1 = format!("Msg #{}", i);
            let m2 = format!("Msg #{} (contd.)", i);
            let message = Multipart::from(vec![msg(m1.as_bytes()), msg(m2.as_bytes())]);
            progress.set_phase("sending request");
            req_sock.send(message).await?;
            progress.set_phase("receiving reply");
            let multipart = req_sock.recv().await?;
            let expected: Multipart = vec![msg(m1.as_bytes()), msg(m2.as_bytes())].into();
            assert_eq!(expected, multipart);
            progress.advance(i + 1);
        }

        progress.phase("joining echo");
        echo.join().unwrap();
        progress.phase("dropping socket");
        drop(req_sock);
        progress.phase("dropping context");
        drop(ctx);
        progress.phase("done");

        Ok(())
    })
}
