use log::info;
use r0z_async::{request, Context, Message, Result};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = env::var("RUST_LOG").unwrap_or_else(|_| "request=DEBUG".to_owned());
    pretty_env_logger::formatted_builder()
        .parse_filters(&filter)
        .init();

    let mut send_sock = request(&Context::new()).connect("tcp://127.0.0.1:7897")?;

    let mut i = 0u32;
    loop {
        let message = format!("Req#{}", i);
        i += 1;

        info!("Request: {:?}", message);
        let message: Message = message.as_bytes().into();
        send_sock.send(message.into()).await?;
        let msg = send_sock.recv().await?;
        info!(
            "Reply: {:?}",
            msg.iter()
                .map(|item| item.as_str().unwrap_or("invalid text"))
                .collect::<Vec<&str>>()
        );
    }
}
