//! Send and receive with async-io, without a Tokio runtime.
use futures::{SinkExt, StreamExt};
use r0rz_async::{pull, push, runtime::AsyncIo, Context, Result};

fn main() -> Result<()> {
    async_io::block_on(async {
        let context = Context::new();
        let mut receiver = pull(&context)
            .with_runtime::<AsyncIo>()
            .set_linger(0)
            .bind("inproc://async-io-example")?;
        let mut sender = push(&context)
            .with_runtime::<AsyncIo>()
            .set_linger(0)
            .connect("inproc://async-io-example")?;
        sender.send(vec!["hello", "async-io"]).await?;
        let message = receiver.next().await.unwrap()?;
        assert_eq!(message[0].as_str(), Some("hello"));
        assert_eq!(message[1].as_str(), Some("async-io"));
        Ok(())
    })
}
