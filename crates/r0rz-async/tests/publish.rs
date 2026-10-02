#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use r0rz::Context;
use r0rz_async::Result;
use std::time::Duration;

use utils::{generate_tcp_address, send_multiparts, sync_receive_subscribe};

mod utils;

#[test]
fn send_single_message() -> Result<()> {
    utils::run(|| async {
        let address = generate_tcp_address();
        let ctx = Context::new();
        let sock = utils::publish(&ctx).bind(&address)?;

        let topic = "topic2";
        let data = vec![vec![topic, "hello", "world"]];
        let (thread, barrier) = sync_receive_subscribe(address, topic.to_owned(), data.clone());

        barrier.wait();
        std::thread::sleep(Duration::from_millis(1000)); // hack to let the subscriber prepare
        send_multiparts(sock, data).await?;

        thread.join().unwrap();

        Ok(())
    })
}
