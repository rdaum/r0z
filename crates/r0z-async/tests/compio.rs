#![cfg(all(unix, feature = "compio"))]

use compio::runtime::{CancelToken, FutureExt};
use futures::{SinkExt, StreamExt};
use r0z_async::{
    AsZmqSocket, Context, Result, SocketExt, TmqError,
    runtime::{Compio, Registration, Runtime},
};
use std::{sync::mpsc, task::Poll, time::Duration};

mod utils;

fn assert_cancelled<T: std::fmt::Debug>(result: Result<T>) {
    assert!(
        matches!(result, Err(TmqError::Io(ref e)) if e.raw_os_error() == Some(libc::ECANCELED)),
        "{result:?}"
    );
}

#[test]
fn token_listeners_do_not_retain_completed_tasks() -> Result<()> {
    struct Task;
    // Waker::noop cannot expose whether the adapter retains a task's Arc.
    #[allow(clippy::manual_noop_waker)]
    impl std::task::Wake for Task {
        fn wake(self: std::sync::Arc<Self>) {}
    }
    let _deadline = utils::Deadline::start();
    utils::compio_runtime()?.block_on(async {
        for cancel in [false, true] {
            let context = Context::new();
            let mut receiver = r0z_async::pull(&context)
                .with_runtime::<Compio>()
                .bind("inproc://receive-token-waker")?;
            let token = CancelToken::new();
            let task = std::sync::Arc::new(Task);
            let weak = std::sync::Arc::downgrade(&task);
            let waker = std::task::Waker::from(task);
            let mut cx = std::task::Context::from_waker(&waker);
            {
                let receive = receiver.next().with_cancel(token.clone());
                futures::pin_mut!(receive);
                assert!(receive.as_mut().poll(&mut cx).is_pending());
                let sender = context.socket(r0z::PUSH)?;
                sender.connect("inproc://receive-token-waker")?;
                sender.send("message", 0)?;
                if cancel {
                    token.cancel();
                }
                let Poll::Ready(Some(result)) = receive.poll(&mut cx) else {
                    panic!("receive did not finish");
                };
                if cancel {
                    assert_cancelled(result);
                } else {
                    assert_eq!(result?, vec!["message"].into());
                }
            }
            drop(waker);
            assert!(weak.upgrade().is_none(), "receive retained its task");

            let mut sender = r0z_async::push(&context)
                .with_runtime::<Compio>()
                .bind("inproc://send-token-waker")?;
            let token = CancelToken::new();
            let task = std::sync::Arc::new(Task);
            let weak = std::sync::Arc::downgrade(&task);
            let waker = std::task::Waker::from(task);
            let mut cx = std::task::Context::from_waker(&waker);
            {
                let send = sender.send(vec!["message"]).with_cancel(token.clone());
                futures::pin_mut!(send);
                assert!(send.as_mut().poll(&mut cx).is_pending());
            }
            let receiver = context.socket(r0z::PULL)?;
            receiver.connect("inproc://send-token-waker")?;
            // Process the inproc connection before retrying the send.
            assert!(sender.get_socket().get_events()?.contains(r0z::POLLOUT));
            if cancel {
                token.clone().cancel();
            }
            {
                let flush = SinkExt::<r0z_async::Multipart>::flush(&mut sender).with_cancel(token);
                futures::pin_mut!(flush);
                let Poll::Ready(result) = flush.poll(&mut cx) else {
                    panic!("send did not finish");
                };
                if cancel {
                    assert_cancelled(result);
                } else {
                    result?;
                    assert_eq!(receiver.recv_msg(r0z::DONTWAIT)?.as_str(), Some("message"));
                }
            }
            drop(waker);
            assert!(weak.upgrade().is_none(), "send retained its task");
        }
        Ok(())
    })
}

#[test]
fn cancelled_wait_releases_endpoint_before_another_driver_turn() -> Result<()> {
    let _deadline = utils::Deadline::start();
    utils::compio_runtime()?.block_on(async {
        let context = Context::new();
        let mut receiver = r0z_async::pull(&context)
            .with_runtime::<Compio>()
            .bind("tcp://127.0.0.1:*")?;
        let endpoint = receiver.get_socket().get_last_endpoint()?.unwrap();
        assert!(
            compio::time::timeout(Duration::from_millis(20), receiver.next())
                .await
                .is_err()
        );
        drop(receiver);
        drop(context);
        // No await between closing the old context and rebinding its endpoint.
        let context = Context::new();
        let replacement = r0z_async::pull(&context)
            .with_runtime::<Compio>()
            .bind(&endpoint)?;
        drop(replacement);
        drop(context);
        Ok(())
    })
}

#[test]
fn cancellation_wakes_receive_and_allows_a_new_token() -> Result<()> {
    let _deadline = utils::Deadline::start();
    utils::compio_runtime()?.block_on(async {
        let context = Context::new();
        let mut receiver = r0z_async::pull(&context)
            .with_runtime::<Compio>()
            .bind("inproc://cancel-receive")?;
        let token = CancelToken::new();
        let cancel = token.clone();
        compio::runtime::spawn(async move {
            compio::time::sleep(Duration::from_millis(20)).await;
            cancel.cancel();
        })
        .detach();
        let received = compio::time::timeout(
            Duration::from_secs(1),
            receiver.next().with_cancel(token.clone()),
        )
        .await
        .expect("cancellation did not wake receiver")
        .unwrap();
        assert_cancelled(received);

        let mut sender = r0z_async::push(&context)
            .with_runtime::<Compio>()
            .connect("inproc://cancel-receive")?;
        let token = CancelToken::new();
        let receive = receiver.next().with_cancel(token.clone());
        futures::pin_mut!(receive);
        assert!(futures::poll!(&mut receive).is_pending());
        sender.send(vec!["after", "cancellation"]).await?;
        let received = receive.await.unwrap()?;
        assert_eq!(received, vec!["after", "cancellation"].into());
        Ok(())
    })
}

#[test]
fn cancelling_dropped_receive_does_not_cancel_its_replacement() -> Result<()> {
    let _deadline = utils::Deadline::start();
    utils::compio_runtime()?.block_on(async {
        let context = Context::new();
        let mut receiver = r0z_async::pull(&context)
            .with_runtime::<Compio>()
            .bind("inproc://replace-cancel")?;
        let token = CancelToken::new();
        {
            let receive = receiver.next().with_cancel(token.clone());
            futures::pin_mut!(receive);
            assert!(futures::poll!(&mut receive).is_pending());
        }
        let receive = receiver.next();
        futures::pin_mut!(receive);
        assert!(futures::poll!(&mut receive).is_pending());
        token.cancel();
        assert!(futures::poll!(&mut receive).is_pending());
        let mut sender = r0z_async::push(&context)
            .with_runtime::<Compio>()
            .connect("inproc://replace-cancel")?;
        sender.send(vec!["replacement"]).await?;
        assert_eq!(receive.await.unwrap()?, vec!["replacement"].into());
        Ok(())
    })
}

#[test]
fn cancelled_receive_preserves_buffered_multiparts() -> Result<()> {
    let _deadline = utils::Deadline::start();
    utils::compio_runtime()?.block_on(async {
        let context = Context::new();
        let mut receiver = r0z_async::pull(&context)
            .with_runtime::<Compio>()
            .bind("inproc://cancel-buffered")?
            .buffered(2);
        let sender = context.socket(r0z::PUSH)?;
        sender.connect("inproc://cancel-buffered")?;
        sender.send_multipart(["first", "message"], 0)?;
        sender.send_multipart(["second", "message"], 0)?;
        assert_eq!(
            receiver.next().await.unwrap()?,
            vec!["first", "message"].into()
        );
        let token = CancelToken::new();
        token.clone().cancel();
        assert_cancelled(receiver.next().with_cancel(token).await.unwrap());
        assert_eq!(
            receiver.next().await.unwrap()?,
            vec!["second", "message"].into()
        );
        Ok(())
    })
}

#[test]
fn cancelled_send_retains_multipart_for_a_later_flush() -> Result<()> {
    let _deadline = utils::Deadline::start();
    utils::compio_runtime()?.block_on(async {
        let context = Context::new();
        let mut sender = r0z_async::push(&context)
            .with_runtime::<Compio>()
            .bind("inproc://cancel-send-buffer")?;
        let token = CancelToken::new();
        {
            let send = sender
                .send(vec!["retained", "multipart"])
                .with_cancel(token.clone());
            futures::pin_mut!(send);
            assert!(futures::poll!(&mut send).is_pending());
            token.cancel();
            assert_cancelled(send.await);
        }
        let mut receiver = r0z_async::pull(&context)
            .with_runtime::<Compio>()
            .connect("inproc://cancel-send-buffer")?;
        SinkExt::<r0z_async::Multipart>::flush(&mut sender).await?;
        assert_eq!(
            receiver.next().await.unwrap()?,
            vec!["retained", "multipart"].into()
        );
        Ok(())
    })
}

#[test]
fn request_reply_token_errors_follow_the_failed_state_contract() -> Result<()> {
    use r0z_async::request_reply::RequestReplyState;
    let _deadline = utils::Deadline::start();
    utils::compio_runtime()?.block_on(async {
        let context = Context::new();
        let mut client = r0z_async::request(&context)
            .with_runtime::<Compio>()
            .bind("inproc://cancel-request")?;
        let token = CancelToken::new();
        {
            let send = client
                .send(vec!["request"].into())
                .with_cancel(token.clone());
            futures::pin_mut!(send);
            assert!(futures::poll!(&mut send).is_pending());
            token.cancel();
            assert_cancelled(send.await);
        }
        assert_eq!(client.state(), RequestReplyState::Failed);
        assert!(matches!(
            client.flush().await,
            Err(TmqError::InvalidRequestReplyState { .. })
        ));
        client.set_linger(0)?;

        let mut server = r0z_async::reply(&context)
            .with_runtime::<Compio>()
            .bind("inproc://cancel-reply")?;
        let token = CancelToken::new();
        {
            let receive = server.recv().with_cancel(token.clone());
            futures::pin_mut!(receive);
            assert!(futures::poll!(&mut receive).is_pending());
            token.cancel();
            assert_cancelled(receive.await);
        }
        assert_eq!(server.state(), RequestReplyState::Failed);
        server.set_linger(0)?;
        Ok(())
    })
}

#[test]
fn split_directions_cancel_independently() -> Result<()> {
    let _deadline = utils::Deadline::start();
    utils::compio_runtime()?.block_on(async {
        let context = Context::new();
        let dealer = r0z_async::dealer(&context)
            .with_runtime::<Compio>()
            .bind("inproc://cancel-split")?;
        let (mut sender, mut receiver) = dealer.split::<r0z_async::Multipart>();
        let send_token = CancelToken::new();
        let receive_token = CancelToken::new();
        let send = sender
            .send(vec!["pending"].into())
            .with_cancel(send_token.clone());
        let receive = receiver.next().with_cancel(receive_token.clone());
        futures::pin_mut!(send, receive);
        assert!(futures::poll!(&mut send).is_pending());
        assert!(futures::poll!(&mut receive).is_pending());
        receive_token.cancel();
        assert_cancelled(receive.await.unwrap());
        assert!(futures::poll!(&mut send).is_pending());
        let cancel = send_token.clone();
        compio::runtime::spawn(async move {
            compio::time::sleep(Duration::from_millis(20)).await;
            cancel.cancel();
        })
        .detach();
        assert_cancelled(
            compio::time::timeout(Duration::from_secs(1), send)
                .await
                .expect("cancellation did not wake sender"),
        );
        Ok(())
    })
}

#[test]
fn polling_outside_original_runtime_returns_an_error() -> Result<()> {
    let _deadline = utils::Deadline::start();
    let runtime = utils::compio_runtime()?;
    let context = Context::new();
    let mut receiver = runtime.block_on(async {
        r0z_async::pull(&context)
            .with_runtime::<Compio>()
            .bind("inproc://runtime-affinity")
    })?;
    // Check both a fresh registration and one with an outstanding driver wait.
    for _ in 0..2 {
        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(matches!(
            std::pin::Pin::new(&mut receiver).poll_next_unpin(&mut cx),
            Poll::Ready(Some(Err(TmqError::Io(_))))
        ));
        utils::compio_runtime()?.block_on(async {
            assert!(matches!(receiver.next().await, Some(Err(TmqError::Io(_)))));
        });
        runtime.block_on(async {
            assert!(futures::poll!(receiver.next()).is_pending());
        });
    }
    // Wrong-runtime errors must not consume already available frames either.
    let sender = context.socket(r0z::PUSH)?;
    sender.connect("inproc://runtime-affinity")?;
    sender.send("ready", 0)?;
    utils::compio_runtime()?.block_on(async {
        assert!(matches!(receiver.next().await, Some(Err(TmqError::Io(_)))));
    });
    runtime.block_on(async {
        assert_eq!(receiver.next().await.unwrap()?, vec!["ready"].into());
        Result::Ok(())
    })
}

#[test]
fn registration_requires_a_compio_runtime() {
    let _deadline = utils::Deadline::start();
    let context = Context::new();
    let result = r0z_async::pull(&context)
        .with_runtime::<Compio>()
        .bind("inproc://outside-compio");
    assert!(matches!(result, Err(TmqError::Io(_))));
    // Context shutdown waits for native socket cleanup; the watchdog covers this too.
    drop(context);
}

#[test]
fn selecting_runtime_preserves_deferred_builder_errors() -> Result<()> {
    let _deadline = utils::Deadline::start();
    utils::compio_runtime()?.block_on(async {
        let context = Context::new();
        let result = r0z_async::pull(&context)
            .set_sndhwm(-1)
            .with_runtime::<Compio>()
            .bind("inproc://invalid-builder");
        assert!(matches!(result, Err(TmqError::Zmq(r0z::Error::EINVAL))));
        Ok(())
    })
}

#[test]
fn pending_registration_can_outlive_runtime_handle() -> Result<()> {
    let _deadline = utils::Deadline::start();
    let runtime = utils::compio_runtime()?;
    let registration = runtime.block_on(async {
        let context = Context::new();
        let socket = context.socket(r0z::PULL)?;
        socket.set_linger(0)?;
        socket.bind("inproc://outlive-runtime")?;
        socket.get_events()?;
        let registration = Compio::register(socket)?;
        futures::future::poll_fn(|cx| {
            assert!(registration.poll_readable(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        // Drive the reactor so the wait reaches the OS before dropping either owner.
        compio::time::sleep(Duration::from_millis(1)).await;
        Result::Ok(registration)
    })?;
    drop(runtime);
    // Submit retains its driver and source. Cancellation and final context destruction must
    // remain safe even after leaving the runtime scope and dropping the application's handle.
    drop(registration);
    Ok(())
}

struct OnDrop(mpsc::Sender<()>);
impl Drop for OnDrop {
    fn drop(&mut self) {
        self.0.send(()).unwrap();
    }
}

#[test]
fn runtime_shutdown_cancels_pending_local_socket_tasks() -> Result<()> {
    let _deadline = utils::Deadline::start();
    let (dropped, observe_drop) = mpsc::channel();
    let runtime = utils::compio_runtime()?;
    runtime.block_on(async {
        let mut receiver = r0z_async::pull(&Context::new())
            .with_runtime::<Compio>()
            .bind("inproc://runtime-shutdown")?;
        let guard = OnDrop(dropped);
        let (armed, observe_arm) = futures::channel::oneshot::channel();
        compio::runtime::spawn(async move {
            let _guard = guard;
            let receive = receiver.next();
            futures::pin_mut!(receive);
            assert!(futures::poll!(&mut receive).is_pending());
            armed.send(()).unwrap();
            receive.await
        })
        .detach();
        observe_arm.await.unwrap();
        compio::time::sleep(Duration::from_millis(1)).await;
        Result::Ok(())
    })?;
    drop(runtime);
    observe_drop.recv_timeout(Duration::from_secs(1)).unwrap();
    Ok(())
}

#[test]
fn cancelled_waits_do_not_interfere_with_new_sockets() -> Result<()> {
    let _deadline = utils::Deadline::start();
    utils::compio_runtime()?.block_on(async {
        let context = Context::new();
        for sequence in 0..64 {
            let mut abandoned = r0z_async::pull(&context)
                .with_runtime::<Compio>()
                .bind(&format!("inproc://abandoned-{sequence}"))?;
            {
                let receive = abandoned.next();
                futures::pin_mut!(receive);
                assert!(futures::poll!(&mut receive).is_pending());
            }
            compio::time::sleep(Duration::from_millis(1)).await;
            drop(abandoned);
            let endpoint = format!("inproc://replacement-{sequence}");
            let mut receiver = r0z_async::pull(&context)
                .with_runtime::<Compio>()
                .bind(&endpoint)?;
            let mut sender = r0z_async::push(&context)
                .with_runtime::<Compio>()
                .connect(&endpoint)?;
            let receive = receiver.next();
            futures::pin_mut!(receive);
            assert!(futures::poll!(&mut receive).is_pending());
            let sequence = sequence.to_string();
            sender.send(vec![sequence.as_str(), "payload"]).await?;
            let message = receive.await.unwrap()?;
            assert_eq!(message[0].as_str(), Some(sequence.as_str()));
            assert_eq!(message[1].as_str(), Some("payload"));
        }
        Ok(())
    })
}

#[test]
fn enabling_compio_preserves_other_adapters_send_types() {
    fn assert_send<T: Send>() {}
    fn sockets<R: Runtime>()
    where
        R::Registration: Send,
    {
        assert_send::<r0z_async::dealer::Dealer<R>>();
        assert_send::<r0z_async::pair::Pair<R>>();
        assert_send::<r0z_async::pull::Pull<R>>();
        assert_send::<r0z_async::push::Push<R>>();
        assert_send::<r0z_async::publish::Publish<R>>();
        assert_send::<r0z_async::router::Router<R>>();
        assert_send::<r0z_async::subscribe::Subscribe<R>>();
        assert_send::<r0z_async::subscribe::SubscribeWithoutTopic<R>>();
        assert_send::<r0z_async::request_reply::RequestReply<R>>();
    }
    #[cfg(feature = "tokio")]
    sockets::<r0z_async::runtime::Tokio>();
    #[cfg(feature = "async-io")]
    sockets::<r0z_async::runtime::AsyncIo>();
    sockets::<r0z_async::runtime::NoRuntime>();
}
