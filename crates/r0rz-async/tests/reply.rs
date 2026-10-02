#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use r0rz::Context;
use r0rz_async::Result;
use std::thread::{spawn, JoinHandle};
use utils::generate_tcp_address;

mod utils;

#[test]
fn single_message() -> Result<()> {
    utils::run(|| async {
        let address = generate_tcp_address();
        let ctx = Context::new();
        let mut recv_sock = utils::reply(&ctx).bind(&address)?;

        let part2 = "single_message";
        let echo = sync_requester(address, 1, part2);

        let multipart = recv_sock.recv().await?;
        assert_eq!(multipart.len(), 2);
        assert_eq!(multipart[1].as_str().unwrap(), part2);
        recv_sock.send(multipart).await?;

        echo.join().unwrap();

        Ok(())
    })
}

#[test]
fn hammer_reply() -> Result<()> {
    utils::run(|| async {
        let address = generate_tcp_address();
        let ctx = Context::new();
        let mut recv_sock = utils::reply(&ctx).bind(&address)?;

        let count = 1_000;

        let part2 = "single_message";
        let echo = sync_requester(address, count, part2);

        for _ in 0..count {
            let multipart = recv_sock.recv().await?;
            assert_eq!(multipart.len(), 2);
            assert_eq!(multipart[1].as_str().unwrap(), part2);
            recv_sock.send(multipart).await?;
        }

        echo.join().unwrap();

        Ok(())
    })
}

pub fn sync_requester(address: String, count: u64, part2: &'static str) -> JoinHandle<()> {
    spawn(move || {
        let socket = Context::new().socket(r0rz::SocketType::REQ).unwrap();
        socket.connect(&address).unwrap();

        for i in 0..count {
            let msg = format!("Req#{}", i);
            socket
                .send_multipart([msg.as_bytes(), part2.as_bytes()], 0)
                .unwrap();
            let received = socket.recv_multipart(0).unwrap();
            assert_eq!(&received[0], &msg.as_bytes());
        }
    })
}
