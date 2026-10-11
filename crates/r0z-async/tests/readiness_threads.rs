#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use futures::{channel::oneshot, future::BoxFuture, task::ArcWake, SinkExt, StreamExt};
use r0z_async::{runtime::Runtime, AsZmqSocket, Multipart, Result};
use std::{
    future::Future,
    sync::{mpsc, Arc, Mutex},
    task::{Context, Poll, Waker},
    thread,
    time::Duration,
};

mod utils;

struct ObserveWake {
    task: Waker,
    first_wake: Mutex<Option<mpsc::Sender<()>>>,
}

impl ArcWake for ObserveWake {
    fn wake_by_ref(this: &Arc<Self>) {
        if let Some(signal) = this.first_wake.lock().unwrap().take() {
            let _ = signal.send(());
        }
        this.task.wake_by_ref();
    }
}

async fn observe_wake(signal: mpsc::Sender<()>) -> Waker {
    let mut signal = Some(signal);
    futures::future::poll_fn(|cx| {
        Poll::Ready(futures::task::waker(Arc::new(ObserveWake {
            task: cx.waker().clone(),
            first_wake: Mutex::new(signal.take()),
        })))
    })
    .await
}

fn message(sequence: usize) -> Multipart {
    vec!["", &sequence.to_string(), "payload"].into()
}

fn expect_message(peer: &r0z::Socket, sequence: usize) {
    let received: Multipart = peer
        .recv_multipart(0)
        .unwrap()
        .into_iter()
        .map(r0z::Message::from)
        .collect();
    assert_eq!(received, message(sequence));
}

fn cross_thread_wake_order<R: Runtime>(
    block_on: impl Fn(BoxFuture<'static, Result<()>>) -> Result<()> + Sync,
) -> Result<()> {
    let context = r0z::Context::new();
    let dealer = r0z_async::dealer(&context)
        .with_runtime::<R>()
        .set_sndhwm(1)
        .set_rcvhwm(1)
        .bind("inproc://cross-thread-readiness")?;
    let peer = context.socket(r0z::DEALER)?;
    peer.set_linger(0)?;
    peer.set_sndhwm(1)?;
    peer.set_rcvhwm(1)?;
    peer.set_sndtimeo(2000)?;
    peer.set_rcvtimeo(2000)?;
    peer.connect("inproc://cross-thread-readiness")?;
    assert!(dealer.get_socket().get_events()?.contains(r0z::POLLOUT));
    let (mut sink, mut stream) = dealer.split::<Multipart>();
    let (armed, reader_armed) = mpsc::channel();
    let (read_wake, reader_woken) = mpsc::channel();
    let (write_wake, writer_woken) = mpsc::channel();
    let (blocked, writer_blocked) = mpsc::channel();
    let (received, first_received) = mpsc::channel();
    let (start_writer, writer_started) = oneshot::channel();
    let (resume_reader, reader_resumed) = oneshot::channel();
    let (resume_writer, writer_resumed) = oneshot::channel();
    const COUNT: usize = 8;
    let reader = async move {
        let waker = observe_wake(read_wake).await;
        let first = stream.next();
        futures::pin_mut!(first);
        assert!(first
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        armed.send(thread::current().id()).unwrap();
        // Receiving a wake must not poll the socket again until the controller permits it.
        reader_resumed.await.unwrap();
        assert_eq!(first.await.unwrap()?, message(0));
        received.send(()).unwrap();
        for sequence in 1..COUNT {
            assert_eq!(stream.next().await.unwrap()?, message(sequence));
        }
        Ok(())
    };
    let writer = async move {
        writer_started.await.unwrap();
        let waker = observe_wake(write_wake).await;
        let mut resume = Some(writer_resumed);
        for sequence in 0..COUNT {
            let send = sink.send(message(sequence));
            futures::pin_mut!(send);
            let result = send.as_mut().poll(&mut Context::from_waker(&waker));
            match result {
                Poll::Ready(result) => result?,
                Poll::Pending => {
                    if let Some(resume) = resume.take() {
                        blocked.send((sequence, thread::current().id())).unwrap();
                        resume.await.unwrap();
                    }
                    send.await?;
                }
            }
        }
        assert!(resume.is_none(), "send queue did not fill");
        Ok(())
    };
    thread::scope(|threads| {
        // Each block_on drives its half on a distinct OS thread. The socket's adapter
        // receives notifications from the real Tokio or async-io reactor.
        let reader = threads.spawn(|| block_on(Box::pin(reader)));
        let writer = threads.spawn(|| block_on(Box::pin(writer)));
        let reader_thread = reader_armed.recv_timeout(Duration::from_secs(2)).unwrap();
        peer.send_multipart(["", "0", "payload"], 0).unwrap();
        reader_woken.recv_timeout(Duration::from_secs(2)).unwrap();
        start_writer.send(()).unwrap();
        let (queued, writer_thread) = writer_blocked.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_ne!(reader_thread, writer_thread);
        assert!(queued > 0 && queued < COUNT);
        // The writer has performed I/O while the reader is woken but held before its next poll.
        for sequence in 0..queued {
            expect_message(&peer, sequence);
        }
        writer_woken.recv_timeout(Duration::from_secs(2)).unwrap();
        resume_reader.send(()).unwrap();
        first_received.recv_timeout(Duration::from_secs(2)).unwrap();
        // Now the reader has performed I/O while the writer is woken but held before its next poll.
        resume_writer.send(()).unwrap();
        for sequence in queued..COUNT {
            expect_message(&peer, sequence);
        }
        for sequence in 1..COUNT {
            peer.send_multipart(["", &sequence.to_string(), "payload"], 0)
                .unwrap();
        }
        reader.join().unwrap()?;
        writer.join().unwrap()?;
        Ok(())
    })
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_split_halves_progress_with_delayed_repolls_on_distinct_threads() -> Result<()> {
    let _deadline = utils::Deadline::start();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _enter = runtime.enter();
    cross_thread_wake_order::<r0z_async::runtime::Tokio>(|future| runtime.block_on(future))
}

#[cfg(feature = "async-io")]
#[test]
fn async_io_split_halves_progress_with_delayed_repolls_on_distinct_threads() -> Result<()> {
    let _deadline = utils::Deadline::start();
    cross_thread_wake_order::<r0z_async::runtime::AsyncIo>(async_io::block_on)
}
