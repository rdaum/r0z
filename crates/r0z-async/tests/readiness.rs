#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use futures::{SinkExt, StreamExt};
use r0z_async::{Context, Result};
use std::time::Duration;

mod utils;

#[test]
fn cancelled_receive_can_wait_again() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let mut receiver = utils::pull(&context)
            .set_linger(0)
            .bind("inproc://cancel-receive")?;
        assert!(utils::timeout(Duration::from_millis(20), receiver.next())
            .await
            .is_err());
        let mut sender = utils::push(&context)
            .set_linger(0)
            .connect("inproc://cancel-receive")?;
        sender.send(vec!["after", "cancellation"]).await?;
        let message = receiver.next().await.unwrap()?;
        assert_eq!(message[0].as_str(), Some("after"));
        assert_eq!(message[1].as_str(), Some("cancellation"));
        Ok(())
    })
}

#[test]
fn cancelled_send_preserves_pending_multipart() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let mut sender = utils::push(&context)
            .set_linger(0)
            .set_sndhwm(1)
            .bind("inproc://cancel-send")?;
        {
            let send = sender.send(vec!["first", "", "last"]);
            futures::pin_mut!(send);
            assert!(futures::poll!(send).is_pending());
        }
        let receiver = context.socket(r0z::PULL)?;
        receiver.set_linger(0)?;
        receiver.set_rcvtimeo(2000)?;
        receiver.connect("inproc://cancel-send")?;
        // Resume the buffered send, rather than enqueueing a second copy.
        futures::SinkExt::<Vec<&str>>::flush(&mut sender).await?;
        assert_eq!(
            receiver.recv_multipart(0)?,
            vec![b"first".to_vec(), vec![], b"last".to_vec()]
        );
        assert_eq!(
            receiver.recv_msg(r0z::DONTWAIT).unwrap_err(),
            r0z::Error::EAGAIN
        );
        Ok(())
    })
}

#[test]
fn idle_socket_does_not_busy_poll() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let mut receiver = utils::pull(&context).set_linger(0).bind("inproc://idle")?;
        let mut polls = 0;
        let receive = futures::future::poll_fn(|cx| {
            polls += 1;
            futures::Stream::poll_next(std::pin::Pin::new(&mut receiver), cx)
        });
        assert!(utils::timeout(Duration::from_millis(30), receive)
            .await
            .is_err());
        assert!(polls < 20, "idle socket was polled {polls} times");
        Ok(())
    })
}

#[cfg(all(feature = "tokio", feature = "async-io"))]
#[test]
fn adapters_can_coexist() -> Result<()> {
    use r0z_async::AsZmqSocket;

    let _deadline = utils::Deadline::start();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let context = Context::new();
        let mut receiver = r0z_async::pull(&context)
            .with_runtime::<r0z_async::runtime::AsyncIo>()
            .set_linger(0)
            .bind("tcp://127.0.0.1:*")?;
        let address = receiver.get_socket().get_last_endpoint()?.unwrap();
        let (armed, wait_for_reader) = futures::channel::oneshot::channel();
        let reader = tokio::spawn(async move {
            let receive = receiver.next();
            futures::pin_mut!(receive);
            assert!(futures::poll!(&mut receive).is_pending());
            armed.send(()).unwrap();
            receive.await.unwrap()
        });
        wait_for_reader.await.unwrap();
        // The default is Tokio. Its sender wakes an async-io registration polled by a Tokio task.
        let mut sender = r0z_async::push(&context).set_linger(0).connect(&address)?;
        sender.send(vec!["mixed"]).await?;
        let message = tokio::time::timeout(Duration::from_secs(2), reader)
            .await
            .unwrap()
            .unwrap()?;
        assert_eq!(message[0].as_str(), Some("mixed"));
        Ok(())
    })
}
