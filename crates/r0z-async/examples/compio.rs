use futures::{SinkExt, StreamExt};
use r0z_async::{Context, Result, pull, push, runtime::Compio};

#[compio::main]
async fn main() -> Result<()> {
    let context = Context::new();
    let mut receiver = pull(&context)
        .with_runtime::<Compio>()
        .bind("inproc://compio-example")?;
    let mut sender = push(&context)
        .with_runtime::<Compio>()
        .connect("inproc://compio-example")?;

    let receive = compio::runtime::spawn(async move {
        let message = receiver.next().await.unwrap()?;
        assert_eq!(message, vec!["hello", "compio"].into());
        Result::Ok(())
    });
    sender.send(vec!["hello", "compio"]).await?;
    receive.await.unwrap()?;
    Ok(())
}
