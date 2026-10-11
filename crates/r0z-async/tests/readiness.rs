#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use futures::{task::ArcWake, SinkExt, Stream, StreamExt};
use r0z_async::{AsZmqSocket, Context, Multipart, Result};
use std::{
    cell::RefCell,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context as TaskContext, Poll, Waker},
    time::Duration,
};

mod utils;

struct TaskWaker;

impl ArcWake for TaskWaker {
    fn wake_by_ref(_: &Arc<Self>) {}
}

#[derive(Default)]
struct CountWakes(AtomicUsize);

impl ArcWake for CountWakes {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::SeqCst);
    }
}

// Keep real native sockets, but withhold reactor notifications. A socket operation must
// wake the opposite task itself when that operation consumes the relevant native edge.
struct SilentRuntime;

struct SilentRegistration {
    socket: r0z::Socket,
    waker: RefCell<Option<Waker>>,
}

impl r0z_async::runtime::Runtime for SilentRuntime {
    fn register(socket: r0z::Socket) -> Result<Box<dyn r0z_async::runtime::Registration>> {
        Ok(Box::new(SilentRegistration {
            socket,
            waker: RefCell::new(None),
        }))
    }
}

impl r0z_async::runtime::Registration for SilentRegistration {
    fn socket(&self) -> &r0z::Socket {
        &self.socket
    }

    fn poll_readable(&self, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        self.waker.replace(Some(cx.waker().clone()));
        Poll::Pending
    }
}

#[test]
fn send_wakes_waiting_receiver_without_a_reactor_notification() -> Result<()> {
    let _deadline = utils::Deadline::start();
    let context = Context::new();
    let dealer = r0z_async::dealer(&context)
        .with_runtime::<SilentRuntime>()
        .bind("inproc://send-wakes-receiver")?;
    let peer = context.socket(r0z::DEALER)?;
    peer.set_linger(0)?;
    peer.connect("inproc://send-wakes-receiver")?;
    let (mut sink, mut stream) = dealer.split::<Multipart>();
    let reader = Arc::new(CountWakes::default());
    let read_waker = futures::task::waker(reader.clone());
    let mut read_cx = TaskContext::from_waker(&read_waker);
    assert!(Pin::new(&mut stream).poll_next(&mut read_cx).is_pending());
    reader.0.store(0, Ordering::SeqCst);

    peer.send("incoming", r0z::DONTWAIT)?;
    let write_waker = futures::task::noop_waker();
    let mut write_cx = TaskContext::from_waker(&write_waker);
    let send = sink.send(vec!["outgoing"].into());
    futures::pin_mut!(send);
    assert!(matches!(
        std::future::Future::poll(send.as_mut(), &mut write_cx),
        Poll::Ready(Ok(()))
    ));
    assert!(
        reader.0.load(Ordering::SeqCst) > 0,
        "send lost the reader wakeup"
    );
    let Poll::Ready(Some(message)) = Pin::new(&mut stream).poll_next(&mut read_cx) else {
        panic!("incoming message was not ready");
    };
    assert_eq!(message?, vec!["incoming"].into());
    assert_eq!(peer.recv_string(r0z::DONTWAIT)?.unwrap(), "outgoing");
    Ok(())
}

#[test]
fn receive_wakes_waiting_sender_without_a_reactor_notification() -> Result<()> {
    let _deadline = utils::Deadline::start();
    let context = Context::new();
    let dealer = r0z_async::dealer(&context)
        .with_runtime::<SilentRuntime>()
        .set_sndhwm(1)
        .set_rcvhwm(1)
        .bind("inproc://receive-wakes-sender")?;
    let peer = context.socket(r0z::DEALER)?;
    peer.set_linger(0)?;
    peer.set_sndhwm(1)?;
    peer.set_rcvhwm(1)?;
    peer.connect("inproc://receive-wakes-sender")?;
    let (mut sink, mut stream) = dealer.split::<Multipart>();
    let writer = Arc::new(CountWakes::default());
    let write_waker = futures::task::waker(writer.clone());
    let mut write_cx = TaskContext::from_waker(&write_waker);
    let mut queued = 0;
    for _ in 0..8 {
        let send = sink.send(vec!["outgoing"].into());
        futures::pin_mut!(send);
        match std::future::Future::poll(send.as_mut(), &mut write_cx) {
            Poll::Ready(result) => {
                result?;
                queued += 1;
            }
            Poll::Pending => break,
        }
    }
    assert!(queued > 0 && queued < 8, "send queue did not fill");
    writer.0.store(0, Ordering::SeqCst);
    for _ in 0..queued {
        assert_eq!(peer.recv_string(r0z::DONTWAIT)?.unwrap(), "outgoing");
    }
    peer.send("incoming", r0z::DONTWAIT)?;
    let read_waker = futures::task::noop_waker();
    let mut read_cx = TaskContext::from_waker(&read_waker);
    let Poll::Ready(Some(message)) = Pin::new(&mut stream).poll_next(&mut read_cx) else {
        panic!("incoming message was not ready");
    };
    assert_eq!(message?, vec!["incoming"].into());
    assert!(
        writer.0.load(Ordering::SeqCst) > 0,
        "receive lost the writer wakeup"
    );
    assert!(matches!(
        futures::Sink::<Multipart>::poll_flush(Pin::new(&mut sink), &mut write_cx),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(peer.recv_string(r0z::DONTWAIT)?.unwrap(), "outgoing");
    Ok(())
}

#[test]
fn completed_receive_releases_task_waker() -> Result<()> {
    utils::run(|| async {
        for buffered in [false, true] {
            let context = Context::new();
            let receiver = utils::pull(&context).bind("inproc://receive-waker")?;
            let mut receiver: Pin<Box<dyn futures::Stream<Item = Result<Multipart>>>> = if buffered
            {
                Box::pin(receiver.buffered(2))
            } else {
                Box::pin(receiver)
            };
            let task = Arc::new(TaskWaker);
            let weak = Arc::downgrade(&task);
            let waker = futures::task::waker(task);
            let mut cx = TaskContext::from_waker(&waker);
            assert!(receiver.as_mut().poll_next(&mut cx).is_pending());

            // Poll again without yielding to the reactor, as a task with other ready work can do.
            let sender = context.socket(r0z::PUSH)?;
            sender.set_linger(0)?;
            sender.connect("inproc://receive-waker")?;
            sender.send_multipart(["first", "", "last"], r0z::DONTWAIT)?;
            let Poll::Ready(Some(message)) = receiver.as_mut().poll_next(&mut cx) else {
                panic!("queued multipart was not received");
            };
            assert_eq!(message?, vec!["first", "", "last"].into());
            drop(waker);
            assert!(
                weak.upgrade().is_none(),
                "completed receive retained its task"
            );
        }
        Ok(())
    })
}

#[test]
fn completed_send_releases_task_waker() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let mut sender = utils::push(&context).bind("inproc://send-waker")?;
        let task = Arc::new(TaskWaker);
        let weak = Arc::downgrade(&task);
        let waker = futures::task::waker(task);
        let mut cx = TaskContext::from_waker(&waker);
        {
            let send = sender.send(vec!["first", "", "last"]);
            futures::pin_mut!(send);
            assert!(std::future::Future::poll(send.as_mut(), &mut cx).is_pending());
        }
        let receiver = context.socket(r0z::PULL)?;
        receiver.set_linger(0)?;
        receiver.connect("inproc://send-waker")?;
        assert!(sender.get_socket().get_events()?.contains(r0z::POLLOUT));
        let Poll::Ready(result) =
            futures::Sink::<Multipart>::poll_flush(Pin::new(&mut sender), &mut cx)
        else {
            panic!("writable socket did not flush");
        };
        result?;
        assert_eq!(
            receiver.recv_multipart(r0z::DONTWAIT)?,
            vec![b"first".to_vec(), vec![], b"last".to_vec()]
        );
        drop(waker);
        assert!(weak.upgrade().is_none(), "completed send retained its task");
        Ok(())
    })
}

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

#[test]
fn receive_rearms_after_draining_a_burst() -> Result<()> {
    utils::run(|| async {
        for buffered in [false, true] {
            let context = Context::new();
            let receiver = utils::pull(&context).bind("inproc://burst-rearm")?;
            let mut receiver: Pin<Box<dyn Stream<Item = Result<Multipart>>>> = if buffered {
                Box::pin(receiver.buffered(4))
            } else {
                Box::pin(receiver)
            };
            let sender = context.socket(r0z::PUSH)?;
            sender.set_linger(0)?;
            sender.connect("inproc://burst-rearm")?;
            for _ in 0..3 {
                for _ in 0..8 {
                    sender.send_multipart(["first", "", "last"], r0z::DONTWAIT)?;
                }
                for _ in 0..8 {
                    assert_eq!(
                        receiver.next().await.unwrap()?,
                        vec!["first", "", "last"].into()
                    );
                }
                let mut polls = 0;
                let receive = futures::future::poll_fn(|cx| {
                    polls += 1;
                    receiver.as_mut().poll_next(cx)
                });
                assert!(utils::timeout(Duration::from_millis(30), receive)
                    .await
                    .is_err());
                assert!(polls < 20, "drained socket was polled {polls} times");

                let receive = receiver.next();
                futures::pin_mut!(receive);
                assert!(futures::poll!(&mut receive).is_pending());
                sender.send("after idle", r0z::DONTWAIT)?;
                let message = utils::timeout(Duration::from_secs(2), receive)
                    .await
                    .unwrap()
                    .unwrap()?;
                assert_eq!(message, vec!["after idle"].into());
            }
        }
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
