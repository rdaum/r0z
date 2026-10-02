#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use futures::SinkExt;
use r0z_async::{Context, Multipart, Result};

mod utils;

fn multipart() -> Multipart {
    [vec![1; 4096], vec![], vec![2; 16], vec![3; 8192]]
        .into_iter()
        .map(r0z::Message::from)
        .collect()
}

#[test]
fn cancelled_send_transfers_original_buffers_after_flush() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let mut sender = utils::push(&context).bind("inproc://owned-cancel")?;
        let message = multipart();
        let pointers: Vec<_> = message.iter().map(|frame| frame.as_ptr()).collect();
        {
            let send = sender.send(message);
            futures::pin_mut!(send);
            assert!(futures::poll!(&mut send).is_pending());
        }
        let receiver = context.socket(r0z::PULL)?;
        receiver.set_rcvtimeo(2000)?;
        receiver.connect("inproc://owned-cancel")?;
        futures::SinkExt::<Multipart>::flush(&mut sender).await?;
        for (index, expected) in multipart().into_iter().enumerate() {
            let received = receiver.recv_msg(0)?;
            assert_eq!(received, expected);
            assert_eq!(received.get_more(), index != 3);
            if received.len() >= 4096 {
                assert_eq!(received.as_ptr(), pointers[index]);
            }
        }
        assert_eq!(
            receiver.recv_msg(r0z::DONTWAIT).unwrap_err(),
            r0z::Error::EAGAIN
        );
        Ok(())
    })
}

#[test]
fn full_queue_preserves_multipart_on_cancellation() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let mut sender = utils::push(&context)
            .set_sndhwm(1)
            .bind("inproc://owned-full")?;
        let receiver = context.socket(r0z::PULL)?;
        receiver.set_rcvhwm(1)?;
        receiver.set_rcvtimeo(2000)?;
        receiver.connect("inproc://owned-full")?;
        // Inproc capacity is the sum of the send and receive high-water marks.
        sender.send(vec!["first"]).await?;
        sender.send(vec!["second"]).await?;
        let message = multipart();
        let pointers: Vec<_> = message.iter().map(|frame| frame.as_ptr()).collect();
        {
            let send = sender.send(message);
            futures::pin_mut!(send);
            assert!(futures::poll!(&mut send).is_pending());
        }
        assert_eq!(receiver.recv_bytes(0)?, b"first");
        assert_eq!(receiver.recv_bytes(0)?, b"second");
        futures::SinkExt::<Multipart>::flush(&mut sender).await?;
        for (index, expected) in multipart().into_iter().enumerate() {
            let received = receiver.recv_msg(0)?;
            assert_eq!(received, expected);
            assert_eq!(received.get_more(), index != 3);
            if received.len() >= 4096 {
                assert_eq!(received.as_ptr(), pointers[index]);
            }
        }
        assert_eq!(
            receiver.recv_msg(r0z::DONTWAIT).unwrap_err(),
            r0z::Error::EAGAIN
        );
        Ok(())
    })
}

#[test]
fn dropping_socket_releases_cancelled_multipart() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let mut sender = utils::push(&context).bind("inproc://owned-drop")?;
        {
            let send = sender.send(multipart());
            futures::pin_mut!(send);
            assert!(futures::poll!(&mut send).is_pending());
        }
        // Valgrind also exercises this path to check release of the retained payloads.
        drop(sender);
        Ok(())
    })
}
