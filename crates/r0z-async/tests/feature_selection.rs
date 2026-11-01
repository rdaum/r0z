use r0z_async::{Context, TmqError};

#[cfg(all(unix, feature = "tokio"))]
#[test]
fn tokio_subscriber_registration_outside_runtime_returns_an_error() {
    let _deadline = utils::Deadline::start();
    let context = Context::new();
    let result = r0z_async::subscribe(&context)
        .with_runtime::<r0z_async::runtime::Tokio>()
        .connect("inproc://subscriber-without-runtime");
    assert!(matches!(result, Err(TmqError::Io(_))));
    let result = r0z_async::subscribe(&context)
        .with_runtime::<r0z_async::runtime::Tokio>()
        .bind("inproc://subscriber-without-runtime");
    assert!(matches!(result, Err(TmqError::Io(_))));
}

#[cfg(all(unix, any(feature = "tokio", feature = "async-io", feature = "compio")))]
mod utils;

#[cfg(not(all(unix, any(feature = "tokio", feature = "async-io", feature = "compio"))))]
#[test]
fn no_default_adapter_returns_an_error() {
    let context = Context::new();
    let result = r0z_async::pull(&context)
        .set_linger(0)
        .bind("inproc://no-runtime");
    assert!(
        matches!(result, Err(TmqError::Io(error)) if error.kind() == std::io::ErrorKind::Unsupported)
    );
}

#[cfg(all(unix, any(feature = "tokio", feature = "async-io", feature = "compio")))]
#[test]
fn default_adapter_sends_and_receives() -> Result<(), TmqError> {
    use futures::{SinkExt, StreamExt};
    let _deadline = utils::Deadline::start();
    let test = async {
        let context = Context::new();
        let mut receiver = r0z_async::pull(&context)
            .set_linger(0)
            .bind("inproc://default")?;
        let mut sender = r0z_async::push(&context)
            .set_linger(0)
            .connect("inproc://default")?;
        sender.send(vec!["default", "adapter"]).await?;
        let message = receiver.next().await.unwrap()?;
        assert_eq!(message[0].as_str(), Some("default"));
        assert_eq!(message[1].as_str(), Some("adapter"));
        Ok(())
    };
    #[cfg(feature = "tokio")]
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(test)
    }
    #[cfg(all(not(feature = "tokio"), feature = "async-io"))]
    {
        async_io::block_on(test)
    }
    #[cfg(all(not(any(feature = "tokio", feature = "async-io")), feature = "compio"))]
    {
        utils::compio_runtime()?.block_on(test)
    }
}
