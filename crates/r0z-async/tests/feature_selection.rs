use r0z_async::{Context, TmqError};

#[cfg(all(unix, any(feature = "tokio", feature = "async-io")))]
mod utils;

#[cfg(not(all(unix, any(feature = "tokio", feature = "async-io"))))]
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

#[cfg(all(unix, any(feature = "tokio", feature = "async-io")))]
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
    #[cfg(not(feature = "tokio"))]
    {
        async_io::block_on(test)
    }
}
