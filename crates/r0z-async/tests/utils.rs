#![allow(dead_code)]
#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use std::thread::{spawn as thread_spawn, JoinHandle};

use futures::{Sink, SinkExt, Stream};
use r0z::{Context, SocketType};

use futures::StreamExt;
use r0z_async::{Multipart, Result, TmqError};
use rand::RngExt;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Barrier, Mutex,
};
use std::{io::Write, time::Instant};

/// Synchronous send and receive functions running in a separate thread.
pub fn sync_send_multiparts<T: Into<r0z::Message> + Send + 'static>(
    address: String,
    socket_type: SocketType,
    multipart: Vec<Vec<T>>,
) -> JoinHandle<()> {
    thread_spawn(move || {
        let socket = Context::new().socket(socket_type).unwrap();
        socket.connect(&address).unwrap();

        for mp in multipart.into_iter() {
            socket
                .send_multipart(mp.into_iter().map(|i| i.into()), 0)
                .unwrap();
        }
    })
}
pub fn sync_send_multipart_repeated<T: Into<r0z::Message> + Clone + 'static + Send>(
    address: String,
    socket_type: SocketType,
    multipart: Vec<T>,
    count: u64,
) -> JoinHandle<()> {
    let progress = Progress::new("sender", count);
    thread_spawn(move || {
        progress.phase("creating socket");
        let context = Context::new();
        let socket = context.socket(socket_type).unwrap();
        progress.phase("connecting");
        socket.connect(&address).unwrap();
        progress.phase("sending");
        for completed in 1..=count {
            let msg = multipart
                .clone()
                .into_iter()
                .map(|i| Into::<r0z::Message>::into(i));
            socket.send_multipart(msg, 0).unwrap();
            progress.advance(completed);
        }
        progress.phase("dropping socket");
        drop(socket);
        progress.phase("dropping context");
        drop(context);
        progress.phase("done");
    })
}
pub fn sync_receive_multiparts<T: Into<r0z::Message> + Send + 'static>(
    address: String,
    socket_type: SocketType,
    expected: Vec<Vec<T>>,
) -> JoinHandle<()> {
    thread_spawn(move || {
        let socket = Context::new().socket(socket_type).unwrap();
        socket.bind(&address).unwrap();

        for item in expected.into_iter() {
            let received: Multipart = socket
                .recv_multipart(0)
                .unwrap()
                .into_iter()
                .map(|i| i.into())
                .collect();
            assert_eq!(
                item.into_iter().map(|i| i.into()).collect::<Multipart>(),
                received
            );
        }
    })
}
pub fn sync_receive_multipart_repeated<T: Into<r0z::Message> + Send + 'static>(
    address: String,
    socket_type: SocketType,
    multipart: Vec<T>,
    count: u64,
) -> JoinHandle<()> {
    thread_spawn(move || {
        let socket = Context::new().socket(socket_type).unwrap();
        socket.bind(&address).unwrap();

        let multipart: Multipart = multipart.into_iter().map(|i| i.into()).collect();
        for _ in 0..count {
            let received = socket.recv_multipart(0).unwrap();
            assert_eq!(
                multipart,
                received
                    .into_iter()
                    .map(|i| i.into())
                    .collect::<Multipart>()
            );
        }
    })
}
pub fn sync_receive_subscribe<T: Into<r0z::Message> + Send + 'static>(
    address: String,
    topic: String,
    expected: Vec<Vec<T>>,
) -> (JoinHandle<()>, Arc<Barrier>) {
    let barrier = Arc::new(Barrier::new(2));
    let handle = barrier.clone();
    (
        thread_spawn(move || {
            let socket = Context::new().socket(r0z::SocketType::SUB).unwrap();
            socket.connect(&address).unwrap();
            socket.set_subscribe(topic.as_bytes()).unwrap();
            handle.wait();

            for item in expected.into_iter() {
                let received: Multipart = socket
                    .recv_multipart(0)
                    .unwrap()
                    .into_iter()
                    .map(|i| i.into())
                    .collect();
                assert_eq!(
                    item.into_iter().map(|i| i.into()).collect::<Multipart>(),
                    received
                );
            }
        }),
        barrier,
    )
}
pub fn sync_echo(address: String, socket_type: SocketType, count: u64) -> JoinHandle<()> {
    thread_spawn(move || {
        let socket = Context::new().socket(socket_type).unwrap();
        socket.bind(&address).unwrap();

        for _ in 0..count {
            let received = socket.recv_multipart(0).unwrap();
            socket.send_multipart(received, 0).unwrap();
        }
    })
}

/// Functions for sending and receiving using the asynchronous sockets.
pub async fn check_receive_multiparts<
    S: Stream<Item = Result<Multipart>> + Unpin,
    T: Into<r0z::Message>,
>(
    mut stream: S,
    expected: Vec<Vec<T>>,
) -> Result<()> {
    for item in expected.into_iter() {
        if let Some(msg) = stream.next().await {
            assert_eq!(
                msg?,
                item.into_iter().map(|i| i.into()).collect::<Multipart>()
            );
        } else {
            panic!("Stream ended too soon");
        }
    }
    Ok(())
}
pub async fn receive_multipart_repeated<
    S: Stream<Item = Result<Multipart>> + Unpin,
    T: Into<r0z::Message>,
>(
    mut stream: S,
    expected: Vec<T>,
    count: u64,
) -> Result<()> {
    let progress = Progress::new("receiver", count);
    let expected: Multipart = expected.into_iter().map(|i| i.into()).collect();
    progress.phase("receiving");
    for completed in 1..=count {
        if let Some(msg) = stream.next().await {
            assert_eq!(msg?, expected);
            progress.advance(completed);
        } else {
            panic!("Stream ended too soon");
        }
    }
    progress.phase("dropping stream");
    drop(stream);
    progress.phase("stream dropped");
    Ok(())
}
pub async fn send_multiparts<
    S: Sink<Multipart, Error = TmqError> + Unpin,
    T: Into<r0z::Message>,
>(
    mut sink: S,
    messages: Vec<Vec<T>>,
) -> Result<()> {
    for message in messages.into_iter() {
        sink.send(message.into_iter().map(|i| i.into()).collect::<Multipart>())
            .await?;
    }

    Ok(())
}
pub async fn send_multipart_repeated<
    S: Sink<Multipart, Error = TmqError> + Unpin,
    T: Into<r0z::Message> + Clone,
>(
    mut sink: S,
    message: Vec<T>,
    count: u64,
) -> Result<()> {
    for _ in 0..count {
        sink.send(
            message
                .clone()
                .into_iter()
                .map(|i| i.into())
                .collect::<Multipart>(),
        )
        .await?;
    }

    Ok(())
}
pub async fn hammer_receive<S: Stream<Item = Result<Multipart>> + Unpin>(
    stream: S,
    address: String,
    socket_type: SocketType,
) -> Result<()> {
    let count: u64 = 1_000_000;
    let thread = sync_send_multipart_repeated(address, socket_type, vec!["hello", "world"], count);

    receive_multipart_repeated(stream, vec!["hello", "world"], count).await?;

    diagnostic_phase("joining sender");
    thread.join().unwrap();
    diagnostic_phase("sender joined; returning to caller");

    Ok(())
}

/// Helper functions
pub fn generate_tcp_address() -> String {
    let mut rng = rand::rng();
    let port = rng.random_range(2000..65000);
    format!("tcp://127.0.0.1:{}", port)
}

pub fn msg(bytes: &[u8]) -> r0z::Message {
    r0z::Message::from(bytes)
}

#[derive(Clone, Copy)]
enum Adapter {
    #[cfg(feature = "tokio")]
    Tokio,
    #[cfg(feature = "async-io")]
    AsyncIo,
}

thread_local! {
    static DIAGNOSTICS: std::cell::RefCell<Option<Arc<Diagnostics>>> = const { std::cell::RefCell::new(None) };
    static ADAPTER: std::cell::Cell<Option<Adapter>> = const { std::cell::Cell::new(None) };
    #[cfg(feature = "async-io")]
    static EXECUTOR: async_executor::Executor<'static> = const { async_executor::Executor::new() };
}

struct Diagnostics {
    test: String,
    started: Instant,
    adapter: Mutex<&'static str>,
    phase: Mutex<&'static str>,
    progress: Mutex<Vec<Arc<ProgressState>>>,
}

impl Diagnostics {
    fn emit(&self, event: &str) {
        let mut line = format!(
            "[async-test {} +{:.3}s] adapter={} phase={} {event}",
            self.test,
            self.started.elapsed().as_secs_f64(),
            self.adapter.lock().unwrap(),
            self.phase.lock().unwrap(),
        );
        for progress in self.progress.lock().unwrap().iter() {
            line.push_str(&format!(
                " | {}={}/{} ({})",
                progress.role,
                progress.completed.load(Ordering::Relaxed),
                progress.total,
                progress.phase.lock().unwrap(),
            ));
        }
        // Direct stderr writes bypass libtest capture, which is lost on process::abort.
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(stderr, "{line}");
        let _ = stderr.flush();
    }
}

struct ProgressState {
    role: &'static str,
    total: u64,
    completed: AtomicU64,
    phase: Mutex<&'static str>,
}

pub struct Progress {
    diagnostics: Arc<Diagnostics>,
    state: Arc<ProgressState>,
}

impl Progress {
    pub fn new(role: &'static str, total: u64) -> Self {
        let diagnostics = DIAGNOSTICS.with(|slot| slot.borrow().clone().expect("active deadline"));
        let state = Arc::new(ProgressState {
            role,
            total,
            completed: AtomicU64::new(0),
            phase: Mutex::new("starting"),
        });
        diagnostics.progress.lock().unwrap().push(state.clone());
        let progress = Self { diagnostics, state };
        progress.diagnostics.emit("progress tracking started");
        progress
    }

    pub fn advance(&self, completed: u64) {
        self.state.completed.store(completed, Ordering::Relaxed);
        if completed == 1 || completed.is_multiple_of(100_000) || completed == self.state.total {
            self.diagnostics.emit("progress");
        }
    }

    pub fn phase(&self, phase: &'static str) {
        *self.state.phase.lock().unwrap() = phase;
        self.diagnostics.emit("progress phase changed");
    }
}

pub fn diagnostic_phase(phase: &'static str) {
    DIAGNOSTICS.with(|slot| {
        if let Some(diagnostics) = slot.borrow().as_ref() {
            *diagnostics.phase.lock().unwrap() = phase;
            if !diagnostics.progress.lock().unwrap().is_empty() {
                diagnostics.emit("test phase changed");
            }
        }
    });
}

// An OS-thread deadline also covers blocking native calls and socket/context teardown.
pub struct Deadline {
    sender: std::sync::mpsc::Sender<()>,
    thread: Option<JoinHandle<()>>,
    diagnostics: Arc<Diagnostics>,
    previous: Option<Arc<Diagnostics>>,
}
impl Deadline {
    #[track_caller]
    pub fn start() -> Self {
        Self::start_with_timeout(std::time::Duration::from_secs(60))
    }

    #[track_caller]
    pub fn start_with_timeout(timeout: std::time::Duration) -> Self {
        let caller = std::panic::Location::caller();
        let diagnostics = Arc::new(Diagnostics {
            test: format!(
                "{} {}:{}",
                std::thread::current().name().unwrap_or("unnamed"),
                caller.file(),
                caller.line()
            ),
            started: Instant::now(),
            adapter: Mutex::new("manual"),
            phase: Mutex::new("starting"),
            progress: Mutex::new(Vec::new()),
        });
        let previous = DIAGNOSTICS.with(|slot| slot.replace(Some(diagnostics.clone())));
        let report = diagnostics.clone();
        let (sender, receiver) = std::sync::mpsc::channel();
        let thread = thread_spawn(move || {
            let expires = report.started + timeout;
            loop {
                let remaining = expires.saturating_duration_since(Instant::now());
                match receiver.recv_timeout(remaining.min(std::time::Duration::from_secs(5))) {
                    Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if Instant::now() >= expires {
                            report.emit(&format!(
                                "deadline exceeded ({:.3}s); aborting",
                                timeout.as_secs_f64()
                            ));
                            std::process::abort();
                        }
                        if !report.progress.lock().unwrap().is_empty() {
                            report.emit("watchdog snapshot");
                        }
                    }
                }
            }
        });
        Self {
            sender,
            thread: Some(thread),
            diagnostics,
            previous,
        }
    }

    pub fn enter_adapter(&self, adapter: &'static str) {
        *self.diagnostics.adapter.lock().unwrap() = adapter;
        self.diagnostics.progress.lock().unwrap().clear();
        *self.diagnostics.phase.lock().unwrap() = "running";
    }
}
impl Drop for Deadline {
    fn drop(&mut self) {
        let _ = self.sender.send(());
        self.thread.take().unwrap().join().unwrap();
        DIAGNOSTICS.with(|slot| slot.replace(self.previous.take()));
    }
}

#[track_caller]
pub fn run<F, Fut>(test: F) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let deadline = Deadline::start();
    #[cfg(feature = "tokio")]
    {
        deadline.enter_adapter("tokio");
        ADAPTER.set(Some(Adapter::Tokio));
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let result = test().await;
                diagnostic_phase("test returned; dropping runtime");
                result
            })?;
        diagnostic_phase("runtime dropped");
    }
    #[cfg(feature = "async-io")]
    {
        deadline.enter_adapter("async-io");
        ADAPTER.set(Some(Adapter::AsyncIo));
        EXECUTOR.with(|executor| {
            async_io::block_on(executor.run(async {
                let result = test().await;
                diagnostic_phase("test returned; leaving executor");
                result
            }))
        })?;
        diagnostic_phase("executor returned");
    }
    ADAPTER.set(None);
    Ok(())
}

fn select<T>(builder: r0z_async::SocketBuilder<T>) -> r0z_async::SocketBuilder<T>
where
    T: r0z_async::FromZmqSocket<T>,
{
    match ADAPTER.get().expect("test runtime is active") {
        #[cfg(feature = "tokio")]
        Adapter::Tokio => builder.with_runtime::<r0z_async::runtime::Tokio>(),
        #[cfg(feature = "async-io")]
        Adapter::AsyncIo => builder.with_runtime::<r0z_async::runtime::AsyncIo>(),
    }
}

#[derive(Debug)]
pub struct Elapsed;

pub async fn timeout<F: std::future::Future>(
    duration: std::time::Duration,
    future: F,
) -> std::result::Result<F::Output, Elapsed> {
    let timer = async {
        match ADAPTER.get().unwrap() {
            #[cfg(feature = "tokio")]
            Adapter::Tokio => tokio::time::sleep(duration).await,
            #[cfg(feature = "async-io")]
            Adapter::AsyncIo => {
                async_io::Timer::after(duration).await;
            }
        }
    };
    futures::pin_mut!(future, timer);
    match futures::future::select(future, timer).await {
        futures::future::Either::Left((output, _)) => Ok(output),
        futures::future::Either::Right(_) => Err(Elapsed),
    }
}

type Task<T> = std::pin::Pin<
    Box<dyn std::future::Future<Output = std::result::Result<T, &'static str>> + Send>,
>;

pub fn spawn<F>(future: F) -> Task<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    match ADAPTER.get().unwrap() {
        #[cfg(feature = "tokio")]
        Adapter::Tokio => {
            let task = tokio::spawn(future);
            Box::pin(async move { task.await.map_err(|_| "task panicked") })
        }
        #[cfg(feature = "async-io")]
        Adapter::AsyncIo => {
            let task = EXECUTOR.with(|executor| executor.spawn(future));
            Box::pin(async move { Ok(task.await) })
        }
    }
}

pub fn dealer(context: &Context) -> r0z_async::SocketBuilder<r0z_async::dealer::Dealer> {
    select(r0z_async::dealer(context))
}

pub fn pair(context: &Context) -> r0z_async::SocketBuilder<r0z_async::pair::Pair> {
    select(r0z_async::pair(context))
}

pub fn publish(context: &Context) -> r0z_async::SocketBuilder<r0z_async::publish::Publish> {
    select(r0z_async::publish(context))
}

pub fn pull(context: &Context) -> r0z_async::SocketBuilder<r0z_async::pull::Pull> {
    select(r0z_async::pull(context))
}

pub fn push(context: &Context) -> r0z_async::SocketBuilder<r0z_async::push::Push> {
    select(r0z_async::push(context))
}

pub fn reply(
    context: &Context,
) -> r0z_async::SocketBuilder<r0z_async::request_reply::RequestReply> {
    select(r0z_async::reply(context))
}

pub fn request(
    context: &Context,
) -> r0z_async::SocketBuilder<r0z_async::request_reply::RequestReply> {
    select(r0z_async::request(context))
}

pub fn router(context: &Context) -> r0z_async::SocketBuilder<r0z_async::router::Router> {
    select(r0z_async::router(context))
}

pub fn subscribe(
    context: &Context,
) -> r0z_async::SocketBuilder<r0z_async::subscribe::SubscribeWithoutTopic> {
    select(r0z_async::subscribe(context))
}
