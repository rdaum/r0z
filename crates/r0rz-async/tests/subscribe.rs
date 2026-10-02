#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use r0rz::{Context, SocketType};
use r0rz_async::Result;

use futures::StreamExt;
use std::time::Duration;
use utils::generate_tcp_address;
use utils::timeout;

mod utils;

#[test]
fn receive_single_message() -> Result<()> {
    utils::run(|| async {
        let address = generate_tcp_address();
        let ctx = Context::new();
        let topic: &[u8] = b"topic2";

        let mut sub_sock = utils::subscribe(&ctx).connect(&address)?.subscribe(topic)?;
        let data = vec![topic, b"hello", b"world"];
        let pub_sock = Context::new().socket(SocketType::PUB).unwrap();
        pub_sock.bind(&address).unwrap();

        // Subscribe sockets don't know when they're connected by zmq design.
        // Sometimes there's a small delay, which can make this test unstable.
        // To work around it, try up to 5 times with a short timeout.

        for _ in 0usize..5 {
            pub_sock.send_multipart(&data, 0).unwrap();
            if let Ok(Some(Ok(incoming))) =
                timeout(Duration::from_millis(100), sub_sock.next()).await
            {
                assert_eq!(
                    incoming,
                    data.into_iter()
                        .map(r0rz_async::Message::from)
                        .collect::<r0rz_async::Multipart>()
                );
                return Ok(());
            }
        }

        panic!("Didn't receive published message");
    })
}
