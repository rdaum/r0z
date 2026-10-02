use r0rz_async::{reply, request, request_reply::RequestReplyState, Context, Result};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new();
    let mut server = reply(&context).set_linger(0).bind("inproc://timeout")?;
    let mut client = request(&context)
        .set_linger(0)
        .connect("inproc://timeout")?;
    client.send(vec!["request"].into()).await?;
    let message = server.recv().await?;

    tokio::select! {
        result = client.recv() => {
            result?;
            panic!("the peer has not replied yet");
        }
        _ = tokio::time::sleep(Duration::from_millis(20)) => {
            println!("Timed out; the socket still belongs to the caller.");
        }
    }
    assert_eq!(client.state(), RequestReplyState::ReceiveReady);
    // A timeout does not reset REQ sequencing. Resume the outstanding receive.
    server.send(message).await?;
    assert_eq!(client.recv().await?, vec!["request"].into());
    assert_eq!(client.state(), RequestReplyState::SendReady);
    Ok(())
}
