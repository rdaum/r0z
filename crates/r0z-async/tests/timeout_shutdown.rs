#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

// tmq #47 omits peer behaviour and shutdown details. Separate a missing reply from an
// undelivered request, and record the exact phase at which native linger blocks teardown.
use r0z_async::{
    request, request_reply::RequestReply, request_reply::RequestReplyState, runtime::Runtime,
    Context, Result, SocketExt,
};
use std::{
    fs,
    io::{self, Write},
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    delivered: bool,
    linger: Option<i32>,
    discard_after_timeout: bool,
    socket_owns_context: bool,
}

const CASES: &[Case] = &[
    Case {
        name: "silent-peer-default",
        delivered: true,
        linger: None,
        discard_after_timeout: false,
        socket_owns_context: false,
    },
    Case {
        name: "silent-peer-zero",
        delivered: true,
        linger: Some(0),
        discard_after_timeout: false,
        socket_owns_context: false,
    },
    Case {
        name: "queued-default",
        delivered: false,
        linger: None,
        discard_after_timeout: false,
        socket_owns_context: false,
    },
    Case {
        name: "queued-zero",
        delivered: false,
        linger: Some(0),
        discard_after_timeout: false,
        socket_owns_context: false,
    },
    Case {
        name: "queued-discard-after-timeout",
        delivered: false,
        linger: Some(-1),
        discard_after_timeout: true,
        socket_owns_context: false,
    },
    Case {
        name: "queued-default-socket-context",
        delivered: false,
        linger: None,
        discard_after_timeout: false,
        socket_owns_context: true,
    },
    Case {
        name: "queued-zero-socket-context",
        delivered: false,
        linger: Some(0),
        discard_after_timeout: false,
        socket_owns_context: true,
    },
    Case {
        name: "queued-infinite",
        delivered: false,
        linger: Some(-1),
        discard_after_timeout: false,
        socket_owns_context: false,
    },
    Case {
        name: "queued-infinite-socket-context",
        delivered: false,
        linger: Some(-1),
        discard_after_timeout: false,
        socket_owns_context: true,
    },
    Case {
        name: "queued-finite",
        delivered: false,
        linger: Some(100),
        discard_after_timeout: false,
        socket_owns_context: false,
    },
    Case {
        name: "silent-peer-infinite",
        delivered: true,
        linger: Some(-1),
        discard_after_timeout: false,
        socket_owns_context: false,
    },
    Case {
        name: "silent-peer-socket-context",
        delivered: true,
        linger: None,
        discard_after_timeout: false,
        socket_owns_context: true,
    },
];

impl Case {
    fn blocked_phase(self) -> Option<&'static str> {
        if !self.delivered && self.linger == Some(-1) && !self.discard_after_timeout {
            Some(if self.socket_owns_context {
                "drop-socket"
            } else {
                "drop-context"
            })
        } else {
            None
        }
    }
}

fn phase(name: &str) {
    println!("phase: {name}");
    io::stdout().flush().unwrap();
}

async fn receive_timeout(socket: &mut RequestReply, mode: &str) {
    let duration = Duration::from_millis(30);
    match mode {
        #[cfg(feature = "tokio")]
        "tokio-timeout" => {
            assert!(tokio::time::timeout(duration, socket.recv()).await.is_err());
        }
        #[cfg(feature = "tokio")]
        "tokio-select" => {
            tokio::select! {
                result = socket.recv() => panic!("unexpected receive result: {result:?}"),
                _ = tokio::time::sleep(duration) => {}
            }
        }
        #[cfg(feature = "async-io")]
        "async-io-select" => {
            let receive = socket.recv();
            let timer = async_io::Timer::after(duration);
            futures::pin_mut!(receive, timer);
            assert!(matches!(
                futures::future::select(receive, timer).await,
                futures::future::Either::Right(_)
            ));
        }
        _ => panic!("unknown timeout mode: {mode}"),
    }
}

async fn scenario<R: Runtime>(
    case: Case,
    transport: &str,
    mode: &str,
    directory: &Path,
) -> Result<()> {
    phase("setup");
    // The peer has a separate context so it cannot keep the client context alive.
    let peer_context = Context::new();
    let peer = if case.delivered {
        Some(peer_context.socket(r0z::REP)?)
    } else {
        None
    };
    let mut reserved_port = None;
    let address = match transport {
        "ipc" => format!("ipc://{}", directory.join("peer.sock").display()),
        "tcp" => {
            if case.delivered {
                "tcp://127.0.0.1:*".to_owned()
            } else {
                // Reserve the port without a ZeroMQ peer. No ZMTP handshake can complete,
                // so libzmq keeps the request queued even if the TCP connection succeeds.
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let address = format!("tcp://{}", listener.local_addr().unwrap());
                reserved_port = Some(listener);
                address
            }
        }
        _ => panic!("unknown transport: {transport}"),
    };
    let address = if let Some(peer) = &peer {
        peer.set_linger(0)?;
        peer.set_rcvtimeo(2000)?;
        peer.bind(&address)?;
        peer.get_last_endpoint()?.unwrap()
    } else {
        address
    };

    let mut context = Some(Context::new());
    let mut builder = request(context.as_ref().unwrap()).with_runtime::<R>();
    if let Some(linger) = case.linger {
        builder = builder.set_linger(linger);
    }
    let mut socket = builder.connect(&address)?;
    assert_eq!(socket.get_linger()?, case.linger.unwrap_or(0));
    if case.socket_owns_context {
        // Equivalent ownership to request(&Context::new()) in the upstream example.
        drop(context.take());
    }
    phase("send");
    socket.send(vec!["never answered"].into()).await?;
    phase("send-returned");
    if let Some(peer) = &peer {
        phase("peer-receive");
        assert_eq!(peer.recv_multipart(0)?, vec![b"never answered".to_vec()]);
        phase("peer-received");
        // Keep the peer open without replying until client teardown completes.
    }
    phase("receive-timeout");
    receive_timeout(&mut socket, mode).await;
    phase("timeout-returned");
    assert_eq!(socket.state(), RequestReplyState::ReceiveReady);
    assert_eq!(socket.get_linger()?, case.linger.unwrap_or(0));
    if case.discard_after_timeout {
        socket.set_linger(0)?;
    }
    phase("drop-socket");
    drop(socket);
    phase("socket-dropped");
    phase("drop-context");
    drop(context);
    phase("context-dropped");
    drop(peer);
    drop(peer_context);
    drop(reserved_port);
    phase("done");
    Ok(())
}

// Reap the child on failures too; do not leave a blocked native context behind.
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn run_child(index: usize, transport: &str, mode: &str) {
    let case = CASES[index];
    let label = format!("{mode}/{transport}/{}", case.name);
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("child.log");
    let log = fs::File::create(&log_path).unwrap();
    let mut child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "timeout_shutdown_child", "--nocapture"])
            .env("R0Z_TIMEOUT_CASE", index.to_string())
            .env("R0Z_TIMEOUT_TRANSPORT", transport)
            .env("R0Z_TIMEOUT_MODE", mode)
            .env("R0Z_TIMEOUT_DIRECTORY", directory.path())
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut blocked_since = None;
    loop {
        let output = fs::read_to_string(&log_path).unwrap();
        let phases: Vec<_> = output
            .lines()
            .filter_map(|line| line.strip_prefix("phase: "))
            .collect();
        if let Some(status) = child.0.try_wait().unwrap() {
            // Read again after exit: the child can finish between our first read and try_wait.
            let output = fs::read_to_string(&log_path).unwrap();
            let phases: Vec<_> = output
                .lines()
                .filter_map(|line| line.strip_prefix("phase: "))
                .collect();
            assert!(
                status.success(),
                "{label}: child failed ({status})\n{output}"
            );
            assert!(
                case.blocked_phase().is_none(),
                "{label}: undelivered request did not linger\n{output}"
            );
            assert!(
                phases.contains(&"timeout-returned"),
                "{label}: timeout did not return\n{output}"
            );
            assert_eq!(
                phases.last(),
                Some(&"done"),
                "{label}: incomplete shutdown\n{output}"
            );
            return;
        }
        if let Some(expected) = case.blocked_phase() {
            if phases.last() == Some(&expected) {
                assert!(
                    phases.contains(&"timeout-returned"),
                    "{label}: blocked before timeout returned\n{output}"
                );
                let since = blocked_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= Duration::from_millis(200) {
                    // Infinite linger is expected here. The parent kills and reaps this child.
                    eprintln!("{label}: timeout returned; native shutdown remained at {expected}");
                    return;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "{label}: exceeded process deadline; last phase {:?}\n{output}",
            phases.last()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn receive_timeouts_and_shutdown() {
    let modes = [
        #[cfg(feature = "tokio")]
        "tokio-timeout",
        #[cfg(feature = "tokio")]
        "tokio-select",
        #[cfg(feature = "async-io")]
        "async-io-select",
    ];
    for mode in modes {
        for transport in ["ipc", "tcp"] {
            for index in 0..CASES.len() {
                run_child(index, transport, mode);
            }
        }
    }
}

#[test]
fn timeout_shutdown_child() -> Result<()> {
    let Ok(index) = std::env::var("R0Z_TIMEOUT_CASE") else {
        return Ok(());
    };
    let case = CASES[index.parse::<usize>().unwrap()];
    let mode = std::env::var("R0Z_TIMEOUT_MODE").unwrap();
    let transport = std::env::var("R0Z_TIMEOUT_TRANSPORT").unwrap();
    let directory = std::env::var("R0Z_TIMEOUT_DIRECTORY").unwrap();
    match mode.as_str() {
        #[cfg(feature = "tokio")]
        "tokio-timeout" | "tokio-select" => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(scenario::<r0z_async::runtime::Tokio>(
                case,
                &transport,
                &mode,
                Path::new(&directory),
            )),
        #[cfg(feature = "async-io")]
        "async-io-select" => async_io::block_on(scenario::<r0z_async::runtime::AsyncIo>(
            case,
            &transport,
            &mode,
            Path::new(&directory),
        )),
        _ => panic!("unknown timeout mode: {mode}"),
    }
}
