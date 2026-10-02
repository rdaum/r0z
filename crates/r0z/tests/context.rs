#[test]
fn context_io_threads() {
    let ctx = r0z::Context::new();

    assert_eq!(
        ctx.get_io_threads().unwrap(),
        r0z_sys::ZMQ_IO_THREADS_DFLT as i32
    );

    ctx.set_io_threads(0).unwrap();
    assert_eq!(ctx.get_io_threads().unwrap(), 0);

    ctx.set_io_threads(7).unwrap();
    assert_eq!(ctx.get_io_threads().unwrap(), 7);

    assert!(ctx.set_io_threads(-1).is_err());
}

fn assert_terminated(context: &r0z::Context) {
    assert_eq!(context.get_io_threads(), Err(r0z::Error::ETERM));
    assert_eq!(context.set_io_threads(1), Err(r0z::Error::ETERM));
    assert_eq!(context.get_max_sockets(), Err(r0z::Error::ETERM));
    assert_eq!(context.set_max_sockets(32), Err(r0z::Error::ETERM));
    assert!(matches!(context.socket(r0z::PAIR), Err(r0z::Error::ETERM)));
}

#[test]
fn destroy_invalidates_all_clones() {
    timebomb::timeout_ms(
        || {
            let mut context = r0z::Context::new();
            let mut clone = context.clone();
            context.destroy().unwrap();
            assert_terminated(&context);
            assert_terminated(&clone);
            context.destroy().unwrap();
            clone.destroy().unwrap();
            // Final Drop must not terminate an already freed native context.
        },
        10000,
    );
}

#[test]
fn context_methods_return_while_destroy_waits_for_sockets() {
    timebomb::timeout_ms(
        || {
            let mut context = r0z::Context::new();
            let receiver = context.socket(r0z::PULL).unwrap();
            receiver.set_linger(0).unwrap();
            receiver.set_rcvtimeo(2000).unwrap();
            receiver.bind("inproc://context-termination").unwrap();
            let mut clone = context.clone();
            let terminate = std::thread::spawn(move || clone.destroy());
            // ETERM proves that termination has started. The live socket keeps it waiting.
            assert_eq!(receiver.recv_msg(0).unwrap_err(), r0z::Error::ETERM);
            assert_terminated(&context);
            assert_eq!(context.destroy(), Err(r0z::Error::ETERM));
            drop(receiver);
            terminate.join().unwrap().unwrap();
            context.destroy().unwrap();
        },
        10000,
    );
}

#[test]
fn concurrent_destroy_calls_terminate_once() {
    timebomb::timeout_ms(
        || {
            let context = r0z::Context::new();
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
            let threads: Vec<_> = (0..4)
                .map(|_| {
                    let mut context = context.clone();
                    let barrier = barrier.clone();
                    std::thread::spawn(move || {
                        barrier.wait();
                        assert!(matches!(context.destroy(), Ok(()) | Err(r0z::Error::ETERM)));
                    })
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
            assert_terminated(&context);
        },
        10000,
    );
}
