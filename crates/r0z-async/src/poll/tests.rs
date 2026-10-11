use super::*;
use std::{
    io,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Mutex,
    },
    task::Waker,
    thread,
    time::Duration,
};

// This deadline also covers native socket/context cleanup after a failed assertion.
struct Deadline(mpsc::Sender<()>, Option<thread::JoinHandle<()>>);

impl Deadline {
    fn start() -> Self {
        let (stop, stopped) = mpsc::channel();
        let name = thread::current()
            .name()
            .unwrap_or("readiness race")
            .to_owned();
        let watchdog = thread::spawn(move || {
            if stopped.recv_timeout(Duration::from_secs(10)) == Err(mpsc::RecvTimeoutError::Timeout)
            {
                eprintln!("{name}: readiness race deadline exceeded");
                std::process::abort();
            }
        });
        Self(stop, Some(watchdog))
    }
}

impl Drop for Deadline {
    fn drop(&mut self) {
        let _ = self.0.send(());
        self.1.take().unwrap().join().unwrap();
    }
}

#[derive(Default)]
struct CountWakes(AtomicUsize);

impl ArcWake for CountWakes {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum At {
    BeforeReactorRegistration,
    AfterReadinessClear,
    BeforeEventQuery,
}

type Action = Box<dyn FnOnce() + Send>;

// Native queues and ZMQ_EVENTS are real. Only notification delivery is controlled.
// No fd event is delivered unless the test explicitly calls notify().
#[derive(Default)]
struct Notifications {
    waker: Mutex<Option<Waker>>,
    action: Mutex<Option<(At, Action)>>,
    stale: AtomicBool,
    query_next: AtomicBool,
    registrations: AtomicUsize,
}

impl Notifications {
    fn fire(&self, at: At) {
        let action = {
            let mut scheduled = self.action.lock().unwrap();
            if scheduled.as_ref().is_some_and(|(point, _)| *point == at) {
                scheduled.take().map(|(_, action)| action)
            } else {
                None
            }
        };
        if let Some(action) = action {
            action();
        }
    }

    fn notify(&self) {
        let waker = self
            .waker
            .lock()
            .unwrap()
            .clone()
            .expect("registered waker");
        waker.wake();
    }
}

struct ControlledRegistration {
    socket: Socket,
    notifications: Arc<Notifications>,
}

impl Registration for ControlledRegistration {
    fn socket(&self) -> &Socket {
        if self.notifications.query_next.swap(false, Ordering::SeqCst) {
            self.notifications.fire(At::BeforeEventQuery);
        }
        &self.socket
    }

    fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.notifications.fire(At::BeforeReactorRegistration);
        *self.notifications.waker.lock().unwrap() = Some(cx.waker().clone());
        self.notifications
            .registrations
            .fetch_add(1, Ordering::SeqCst);
        self.notifications.query_next.store(true, Ordering::SeqCst);
        if self.notifications.stale.swap(false, Ordering::SeqCst) {
            self.notifications.fire(At::AfterReadinessClear);
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Receive,
    BufferedReceive,
    Send,
}

const OPERATIONS: [Operation; 3] = [
    Operation::Receive,
    Operation::BufferedReceive,
    Operation::Send,
];
const FRAMES: [&str; 3] = ["first", "", "last"];

struct Fixture {
    poller: ZmqPoller,
    peer: Arc<Mutex<Socket>>,
    notifications: Arc<Notifications>,
    receive_buffer: ReceiverBuffer,
    send_buffer: Multipart,
}

impl Fixture {
    fn new() -> Self {
        let context = r0z::Context::new();
        let socket = context.socket(r0z::DEALER).unwrap();
        let peer = context.socket(r0z::DEALER).unwrap();
        for socket in [&socket, &peer] {
            socket.set_linger(0).unwrap();
            socket.set_sndhwm(1).unwrap();
            socket.set_rcvhwm(1).unwrap();
        }
        socket.bind("inproc://readiness-race").unwrap();
        peer.connect("inproc://readiness-race").unwrap();
        assert!(socket.get_events().unwrap().contains(r0z::POLLOUT));
        let notifications = Arc::new(Notifications::default());
        let poller = ZmqPoller {
            registration: Box::new(ControlledRegistration {
                socket,
                notifications: notifications.clone(),
            }),
            waiters: Arc::default(),
            read_waiting: Cell::new(false),
            write_waiting: Cell::new(false),
        };
        Self {
            poller,
            peer: Arc::new(Mutex::new(peer)),
            notifications,
            receive_buffer: ReceiverBuffer::new(2),
            send_buffer: FRAMES.to_vec().into(),
        }
    }

    // Observe EAGAIN, then return the peer operation that will make progress possible.
    fn block(&self, operation: Operation) -> Action {
        let peer = self.peer.clone();
        match operation {
            Operation::Receive | Operation::BufferedReceive => {
                assert_eq!(
                    self.poller
                        .get_socket()
                        .recv_msg(r0z::DONTWAIT)
                        .unwrap_err(),
                    r0z::Error::EAGAIN
                );
                Box::new(move || {
                    peer.lock()
                        .unwrap()
                        .send_multipart(FRAMES, r0z::DONTWAIT)
                        .unwrap()
                })
            }
            Operation::Send => {
                let mut queued = 0;
                loop {
                    match self.poller.get_socket().send("queued", r0z::DONTWAIT) {
                        Ok(()) => queued += 1,
                        Err(r0z::Error::EAGAIN) => break,
                        other => panic!("unexpected send result: {other:?}"),
                    }
                    assert!(queued < 8, "send queue did not fill");
                }
                assert!(queued > 0);
                Box::new(move || {
                    let peer = peer.lock().unwrap();
                    for _ in 0..queued {
                        assert_eq!(
                            peer.recv_msg(r0z::DONTWAIT).unwrap().as_str(),
                            Some("queued")
                        );
                    }
                })
            }
        }
    }

    fn poll(&mut self, operation: Operation, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match operation {
            Operation::Receive => self.poller.multipart_recv(cx).map(|result| {
                assert_eq!(result.unwrap(), FRAMES.to_vec().into());
                Ok(())
            }),
            Operation::BufferedReceive => self
                .poller
                .multipart_recv_buffered(cx, &mut self.receive_buffer)
                .map(|result| {
                    assert_eq!(result.unwrap(), FRAMES.to_vec().into());
                    Ok(())
                }),
            Operation::Send => self.poller.multipart_flush(cx, &mut self.send_buffer),
        }
    }

    fn finish(&mut self, operation: Operation, cx: &mut Context<'_>, wakes: &CountWakes) {
        // A native send can defer command processing until the next event query.
        // Permit that extra retry, but never poll again without a wakeup.
        let mut completed = false;
        for _ in 0..3 {
            assert!(
                wakes.0.swap(0, Ordering::SeqCst) > 0,
                "{operation:?}: retry without a wakeup"
            );
            match self.poll(operation, cx) {
                Poll::Ready(result) => {
                    result.unwrap();
                    completed = true;
                    break;
                }
                Poll::Pending => {}
            }
        }
        assert!(completed, "{operation:?}: operation did not complete");
        if matches!(operation, Operation::Send) {
            let received = self
                .peer
                .lock()
                .unwrap()
                .recv_multipart(r0z::DONTWAIT)
                .unwrap();
            assert_eq!(received, FRAMES.map(|frame| frame.as_bytes().to_vec()));
        }
    }
}

#[test]
fn availability_between_eagain_and_registration_schedules_a_retry() {
    let _deadline = Deadline::start();
    for operation in OPERATIONS {
        let mut fixture = Fixture::new();
        let make_ready = fixture.block(operation);
        make_ready();
        let wakes = Arc::new(CountWakes::default());
        let waker = futures::task::waker(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let direction = if matches!(operation, Operation::Send) {
            r0z::POLLOUT
        } else {
            r0z::POLLIN
        };
        // Enter the same rearm path used after a failed nonblocking operation.
        fixture.poller.rearm(&mut cx, direction).unwrap();
        assert!(
            wakes.0.load(Ordering::SeqCst) > 0,
            "{operation:?}: no retry scheduled"
        );
        fixture.finish(operation, &mut cx, &wakes);
    }
}

#[test]
fn availability_during_registration_and_event_check_schedules_a_retry() {
    let _deadline = Deadline::start();
    for operation in OPERATIONS {
        for at in [
            At::BeforeReactorRegistration,
            At::AfterReadinessClear,
            At::BeforeEventQuery,
        ] {
            let mut fixture = Fixture::new();
            *fixture.notifications.action.lock().unwrap() = Some((at, fixture.block(operation)));
            fixture
                .notifications
                .stale
                .store(at == At::AfterReadinessClear, Ordering::SeqCst);
            let wakes = Arc::new(CountWakes::default());
            let waker = futures::task::waker(wakes.clone());
            let mut cx = Context::from_waker(&waker);
            assert!(fixture.poll(operation, &mut cx).is_pending());
            assert!(
                fixture.notifications.action.lock().unwrap().is_none(),
                "{operation:?} {at:?}: injection did not run"
            );
            assert!(
                wakes.0.load(Ordering::SeqCst) > 0,
                "{operation:?} {at:?}: no retry scheduled"
            );
            fixture.finish(operation, &mut cx, &wakes);
        }
    }
}

#[test]
fn notification_after_event_check_reaches_the_registered_task() {
    let _deadline = Deadline::start();
    for operation in OPERATIONS {
        for stale in [false, true] {
            let mut fixture = Fixture::new();
            let make_ready = fixture.block(operation);
            fixture.notifications.stale.store(stale, Ordering::SeqCst);
            let wakes = Arc::new(CountWakes::default());
            let waker = futures::task::waker(wakes.clone());
            let mut cx = Context::from_waker(&waker);
            assert!(fixture.poll(operation, &mut cx).is_pending());
            assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
            assert_eq!(
                fixture.notifications.registrations.load(Ordering::SeqCst),
                if stale { 2 } else { 1 }
            );
            let notifications = fixture.notifications.clone();
            // Deliver after the readiness check, before allowing the task to poll again.
            thread::spawn(move || {
                make_ready();
                notifications.notify();
            })
            .join()
            .unwrap();
            assert!(
                wakes.0.load(Ordering::SeqCst) > 0,
                "{operation:?}: notification lost"
            );
            fixture.finish(operation, &mut cx, &wakes);
        }
    }
}
