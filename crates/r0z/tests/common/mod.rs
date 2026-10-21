#![allow(dead_code)]

use std::sync::{Once, mpsc};
use std::thread;
use std::time::Duration;

static LOGGER_INIT: Once = Once::new();

#[macro_export]
macro_rules! test {
    ($name:ident, $block:block) => {
        #[test]
        fn $name() {
            $crate::common::ensure_env_logger_initialized();
            $crate::common::timeout_ms(|| $block, 10000);
        }
    };
}

#[macro_export]
macro_rules! test_capability {
    ($name:ident, $capability:literal, $block:block) => {
        #[test]
        fn $name() {
            if r0z::has($capability).unwrap() {
                $crate::common::ensure_env_logger_initialized();
                $crate::common::timeout_ms(|| $block, 10000);
            }
        }
    };
}

pub fn ensure_env_logger_initialized() {
    LOGGER_INIT.call_once(env_logger::init);
}

pub fn timeout_ms<F: FnOnce() + Send + 'static>(test: F, max_ms: u64) {
    let (sender, receiver) = mpsc::channel();
    let worker = thread::spawn(move || {
        test();
        let _ = sender.send(());
    });

    if receiver.recv_timeout(Duration::from_millis(max_ms)) == Err(mpsc::RecvTimeoutError::Timeout)
    {
        // Do not join a worker that can be blocked in a native call or socket teardown.
        panic!("test exceeded {} ms deadline", max_ms);
    }
    if let Err(panic) = worker.join() {
        std::panic::resume_unwind(panic);
    }
}
