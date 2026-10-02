#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use futures::{
    channel::oneshot,
    future::{AbortHandle, Abortable},
    SinkExt, StreamExt,
};
use r0z_async::{AsZmqSocket, Context, Multipart, Result};
use std::{future::Future, task::Poll, time::Duration};

mod utils;

fn message(sequence: usize) -> Multipart {
    vec!["", &sequence.to_string(), "payload"].into()
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Unsplit,
    SplitOneTask,
    SplitSeparateTasks,
}

// Cover the six arrangements in tmq #50 over its original IPC transport and TCP.
#[track_caller]
fn dealer_reply_cases(mode: Mode) -> Result<()> {
    utils::run(|| async move {
        for ipc in [true, false] {
            for dealer_binds in [true, false] {
                eprintln!("DEALER/REP: {mode:?}, ipc={ipc}, dealer_binds={dealer_binds}");
                let directory = tempfile::tempdir().unwrap();
                let endpoint = if ipc {
                    format!("ipc://{}", directory.path().join("peer.sock").display())
                } else {
                    "tcp://127.0.0.1:*".to_owned()
                };
                let context = Context::new();
                let dealer = utils::dealer(&context);
                let reply = utils::reply(&context);
                let (mut dealer, mut reply) = if dealer_binds {
                    let dealer = dealer.bind(&endpoint)?;
                    let address = dealer.get_socket().get_last_endpoint()?.unwrap();
                    (dealer, reply.connect(&address)?)
                } else {
                    let reply = reply.bind(&endpoint)?;
                    let address = reply.get_socket().get_last_endpoint()?.unwrap();
                    (dealer.connect(&address)?, reply)
                };
                const COUNT: usize = 32;
                let (received, wait_for_delivery) = oneshot::channel();
                let peer = utils::spawn(async move {
                    for sequence in 0..COUNT {
                        let request = reply.recv().await.unwrap();
                        let mut expected = message(sequence);
                        expected.pop_front();
                        assert_eq!(request, expected);
                        reply.send(request).await.unwrap();
                    }
                    // Keep zero-linger sockets alive until the final reply is delivered.
                    wait_for_delivery.await.unwrap();
                });
                match mode {
                    Mode::Unsplit => {
                        for sequence in 0..COUNT {
                            dealer.send(message(sequence)).await?;
                            assert_eq!(dealer.next().await.unwrap()?, message(sequence));
                        }
                    }
                    Mode::SplitOneTask => {
                        let (mut sink, mut stream) = dealer.split::<Multipart>();
                        for sequence in 0..COUNT {
                            sink.send(message(sequence)).await?;
                            assert_eq!(stream.next().await.unwrap()?, message(sequence));
                        }
                    }
                    Mode::SplitSeparateTasks => {
                        let (mut sink, mut stream) = dealer.split::<Multipart>();
                        let (armed, wait_for_reader) = oneshot::channel();
                        let reader = utils::spawn(async move {
                            let first = stream.next();
                            futures::pin_mut!(first);
                            assert!(futures::poll!(&mut first).is_pending());
                            armed.send(()).unwrap();
                            assert_eq!(first.await.unwrap().unwrap(), message(0));
                            for sequence in 1..COUNT {
                                assert_eq!(
                                    stream.next().await.unwrap().unwrap(),
                                    message(sequence)
                                );
                            }
                        });
                        let writer = utils::spawn(async move {
                            wait_for_reader.await.unwrap();
                            for sequence in 0..COUNT {
                                sink.send(message(sequence)).await.unwrap();
                            }
                        });
                        reader.await.unwrap();
                        writer.await.unwrap();
                    }
                }
                received.send(()).unwrap();
                peer.await.unwrap();
            }
        }
        Ok(())
    })
}

#[test]
fn dealer_reply_unsplit() -> Result<()> {
    dealer_reply_cases(Mode::Unsplit)
}

#[test]
fn dealer_reply_split_one_task() -> Result<()> {
    dealer_reply_cases(Mode::SplitOneTask)
}

#[test]
fn dealer_reply_split_separate_tasks() -> Result<()> {
    dealer_reply_cases(Mode::SplitSeparateTasks)
}

// Inproc queues with HWM=1 fill before the peer reads. Gate peer traffic on an
// observed Pending send so this exercises backpressure on every run.
#[track_caller]
fn backpressure(cancel_reader: bool) -> Result<()> {
    utils::run(|| async move {
        let context = Context::new();
        let dealer = utils::dealer(&context)
            .set_sndhwm(1)
            .set_rcvhwm(1)
            .bind("inproc://split-backpressure")?;
        let mut peer = utils::dealer(&context)
            .set_sndhwm(1)
            .set_rcvhwm(1)
            .connect("inproc://split-backpressure")?;
        let (mut sink, mut stream) = dealer.split::<Multipart>();
        let (armed, wait_for_reader) = oneshot::channel();
        let (blocked, wait_for_backpressure) = oneshot::channel();
        let (delivered, wait_for_delivery) = oneshot::channel();
        let (abort_reader, registration) = AbortHandle::new_pair();
        const COUNT: usize = 64;
        let reader = utils::spawn(Abortable::new(
            async move {
                let first = stream.next();
                futures::pin_mut!(first);
                assert!(futures::poll!(&mut first).is_pending());
                armed.send(()).unwrap();
                assert_eq!(first.await.unwrap().unwrap(), message(0));
                for sequence in 1..COUNT {
                    assert_eq!(stream.next().await.unwrap().unwrap(), message(sequence));
                }
            },
            registration,
        ));
        let writer = utils::spawn(async move {
            wait_for_reader.await.unwrap();
            let mut blocked = Some(blocked);
            for sequence in 0..COUNT {
                let send = sink.send(message(sequence));
                futures::pin_mut!(send);
                match futures::poll!(&mut send) {
                    Poll::Ready(result) => result.unwrap(),
                    Poll::Pending => {
                        if let Some(blocked) = blocked.take() {
                            blocked.send(sequence).unwrap();
                        }
                        send.await.unwrap();
                    }
                }
            }
            assert!(blocked.is_none(), "the send queue never filled");
            wait_for_delivery.await.unwrap();
        });
        let queued = wait_for_backpressure.await.unwrap();
        assert!(queued > 0 && queued < COUNT);
        let reader = if cancel_reader {
            abort_reader.abort();
            assert!(reader.await.unwrap().is_err());
            None
        } else {
            Some(reader)
        };
        for sequence in 0..COUNT {
            assert_eq!(peer.next().await.unwrap()?, message(sequence));
            if !cancel_reader {
                peer.send(message(sequence)).await?;
            }
        }
        delivered.send(()).unwrap();
        writer.await.unwrap();
        if let Some(reader) = reader {
            reader.await.unwrap().unwrap();
        }
        Ok(())
    })
}

#[test]
fn split_bidirectional_backpressure() -> Result<()> {
    backpressure(false)
}

#[test]
fn cancelled_reader_task_leaves_sender_usable() -> Result<()> {
    backpressure(true)
}

#[test]
fn cancelled_writer_task_leaves_receiver_usable() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let dealer = utils::dealer(&context).bind("inproc://cancel-split-writer")?;
        let (mut sink, mut stream) = dealer.split::<Multipart>();
        let (armed, wait_for_reader) = oneshot::channel();
        let (blocked, wait_for_writer) = oneshot::channel();
        let reader = utils::spawn(async move {
            let receive = stream.next();
            futures::pin_mut!(receive);
            assert!(futures::poll!(&mut receive).is_pending());
            armed.send(()).unwrap();
            assert_eq!(receive.await.unwrap().unwrap(), message(1));
        });
        let (abort_writer, registration) = AbortHandle::new_pair();
        let writer = utils::spawn(Abortable::new(
            async move {
                wait_for_reader.await.unwrap();
                let send = sink.send(message(0));
                futures::pin_mut!(send);
                assert!(futures::poll!(&mut send).is_pending());
                blocked.send(()).unwrap();
                send.await.unwrap();
            },
            registration,
        ));
        wait_for_writer.await.unwrap();
        abort_writer.abort();
        assert!(writer.await.unwrap().is_err());
        let mut peer = utils::dealer(&context).connect("inproc://cancel-split-writer")?;
        peer.send(message(1)).await?;
        reader.await.unwrap();
        Ok(())
    })
}

#[test]
fn cancelled_operations_resume_in_new_tasks() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let dealer = utils::dealer(&context).bind("inproc://resume-split")?;
        let (mut sink, mut stream) = dealer.split::<Multipart>();
        // Leave both directions with wakers from tasks that have finished.
        let mut stream = utils::spawn(async move {
            {
                let receive = stream.next();
                futures::pin_mut!(receive);
                assert!(futures::poll!(&mut receive).is_pending());
            }
            stream
        })
        .await
        .unwrap();
        let mut sink = utils::spawn(async move {
            {
                let send = sink.send(message(0));
                futures::pin_mut!(send);
                assert!(futures::poll!(&mut send).is_pending());
            }
            sink
        })
        .await
        .unwrap();
        let mut peer = utils::dealer(&context).connect("inproc://resume-split")?;
        let reader = utils::spawn(async move {
            for sequence in 0..2 {
                assert_eq!(stream.next().await.unwrap().unwrap(), message(sequence));
            }
        });
        let writer = utils::spawn(async move {
            // Flush the cancelled send before adding another message.
            sink.flush().await.unwrap();
            sink.send(message(1)).await.unwrap();
        });
        for sequence in 0..2 {
            assert_eq!(peer.next().await.unwrap()?, message(sequence));
            peer.send(message(sequence)).await?;
        }
        reader.await.unwrap();
        writer.await.unwrap();
        Ok(())
    })
}

async fn assert_idle(operation: impl Future) {
    futures::pin_mut!(operation);
    let mut polls = 0;
    let counted = futures::future::poll_fn(|cx| {
        polls += 1;
        operation.as_mut().poll(cx)
    });
    assert!(utils::timeout(Duration::from_millis(30), counted)
        .await
        .is_err());
    assert!(polls < 20, "idle split operation was polled {polls} times");
}

#[test]
fn idle_split_halves_do_not_busy_poll() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let dealer = utils::dealer(&context).bind("inproc://idle-split")?;
        let (mut sink, mut stream) = dealer.split::<Multipart>();
        let reader = utils::spawn(async move { assert_idle(stream.next()).await });
        let writer = utils::spawn(async move { assert_idle(sink.send(message(0))).await });
        reader.await.unwrap();
        writer.await.unwrap();
        Ok(())
    })
}
