use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use futures::SinkExt;
use r0z_async::{AsZmqSocket, Multipart};
use std::time::Duration;

const CASES: &[(&str, usize, usize)] = &[
    ("16b", 1, 16),
    ("4k", 1, 4096),
    ("1m", 1, 1024 * 1024),
    ("4x4k", 4, 4096),
];

fn messages(frames: usize, size: usize, count: usize) -> Vec<Multipart> {
    let payload = vec![0x5a; size];
    (0..count)
        .map(|_| {
            (0..frames)
                .map(|_| r0z::Message::from(payload.as_slice()))
                .collect()
        })
        .collect()
}

fn sockets(context: &r0z::Context, tcp: bool) -> (r0z_async::push::Push, r0z::Socket) {
    let receiver = context.socket(r0z::PULL).unwrap();
    receiver.set_linger(0).unwrap();
    receiver.set_rcvtimeo(5000).unwrap();
    receiver
        .bind(if tcp {
            "tcp://127.0.0.1:*"
        } else {
            "inproc://owned-send"
        })
        .unwrap();
    let address = receiver.get_last_endpoint().unwrap().unwrap();
    let sender = r0z_async::push(context).connect(&address).unwrap();
    (sender, receiver)
}

fn receive(receiver: &r0z::Socket, frames: usize, size: usize) {
    for frame in 0..frames {
        let message = receiver.recv_msg(0).unwrap();
        assert_eq!(message.len(), size);
        assert_eq!(message.get_more(), frame + 1 != frames);
        assert_eq!(message[0], 0x5a);
    }
}

// A fixed workload for Valgrind allocation counts, separate from timing samples.
fn allocation_probe(case: &str) {
    let &(_, frames, size) = CASES.iter().find(|&&(name, _, _)| name == case).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _enter = runtime.enter();
    let context = r0z::Context::new();
    let (mut sender, receiver) = sockets(&context, false);
    let mut retained = 0;
    const COUNT: usize = 1000;
    for _ in 0..COUNT {
        let message = messages(frames, size, 1).pop().unwrap();
        let pointers: Vec<_> = message.iter().map(|frame| frame.as_ptr()).collect();
        runtime.block_on(sender.send(message)).unwrap();
        for pointer in pointers {
            let received = receiver.recv_msg(0).unwrap();
            assert_eq!(received.len(), size);
            retained += usize::from(received.as_ptr() == pointer);
        }
    }
    println!(
        "case={case} messages={COUNT} frames={} retained_buffers={retained}",
        COUNT * frames
    );
}

fn main() {
    if std::env::args().nth(1).as_deref() == Some("--allocation-probe") {
        allocation_probe(&std::env::args().nth(2).expect("payload case"));
        return;
    }
    let mut criterion = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(200))
        .measurement_time(Duration::from_secs(1))
        .configure_from_args();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _enter = runtime.enter();
    for tcp in [false, true] {
        let transport = if tcp { "tcp" } else { "inproc" };
        let mut group = criterion.benchmark_group(format!("owned_send/{transport}"));
        for &(name, frames, size) in CASES {
            for batch in [1, 32] {
                let context = r0z::Context::new();
                let (mut sender, receiver) = sockets(&context, tcp);
                // Establish the pipe before measurement.
                runtime
                    .block_on(sender.send(messages(frames, size, 1).pop().unwrap()))
                    .unwrap();
                receive(&receiver, frames, size);
                let (delivered, wait_for_delivery) = std::sync::mpsc::channel();
                let peer = std::thread::spawn(move || loop {
                    let first = receiver.recv_msg(0).unwrap();
                    if first.is_empty() {
                        break;
                    }
                    assert_eq!(first.len(), size);
                    assert_eq!(first.get_more(), frames > 1);
                    for frame in 1..frames {
                        let part = receiver.recv_msg(0).unwrap();
                        assert_eq!(part.len(), size);
                        assert_eq!(part.get_more(), frame + 1 != frames);
                    }
                    for _ in 1..batch {
                        receive(&receiver, frames, size);
                    }
                    delivered.send(()).unwrap();
                });
                group.throughput(Throughput::Bytes((frames * size * batch) as u64));
                group.bench_function(BenchmarkId::new(name, batch), |b| {
                    b.iter_batched(
                        || messages(frames, size, batch),
                        |messages| {
                            runtime.block_on(async {
                                for message in messages {
                                    sender.send(message).await.unwrap();
                                }
                            });
                            // Include delivery and keep queued payloads alive through receipt.
                            wait_for_delivery
                                .recv_timeout(Duration::from_secs(5))
                                .unwrap();
                        },
                        BatchSize::PerIteration,
                    );
                });
                runtime.block_on(sender.send(vec![""])).unwrap();
                peer.join().unwrap();
                // Keep the async socket and native context alive until the peer exits.
                assert_eq!(sender.get_socket().get_linger().unwrap(), 0);
            }
        }
        group.finish();
    }
    criterion.final_summary();
}
