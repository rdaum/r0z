mod common;

use std::panic::catch_unwind;
use std::sync::mpsc;
use std::time::Duration;

#[test]
fn waits_for_completion() {
    let (sender, receiver) = mpsc::channel();
    common::timeout_ms(move || sender.send(42).unwrap(), 10000);
    assert_eq!(receiver.try_recv().unwrap(), 42);
}

#[test]
fn preserves_worker_panic() {
    let panic = catch_unwind(|| common::timeout_ms(|| panic!("worker panic"), 10000)).unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"worker panic"));
}

#[test]
fn times_out_without_joining_blocked_worker() {
    let (release, blocked) = mpsc::channel();
    let (finished, completion) = mpsc::channel();
    let panic = catch_unwind(|| {
        common::timeout_ms(
            move || {
                let _ = blocked.recv_timeout(Duration::from_secs(10));
                finished.send(()).unwrap();
            },
            10,
        );
    });
    assert_eq!(completion.try_recv(), Err(mpsc::TryRecvError::Empty));
    release.send(()).unwrap();
    completion.recv_timeout(Duration::from_secs(10)).unwrap();
    let panic = panic.unwrap_err();
    assert_eq!(
        panic.downcast_ref::<String>().map(String::as_str),
        Some("test exceeded 10 ms deadline")
    );
}
