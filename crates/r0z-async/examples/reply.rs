use log::info;
use r0z_async::{Context, Result, reply};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = env::var("RUST_LOG").unwrap_or_else(|_| "reply=DEBUG".to_owned());
    pretty_env_logger::formatted_builder()
        .parse_filters(&filter)
        .init();

    let mut recv_sock = reply(&Context::new()).bind("tcp://127.0.0.1:7897")?;

    loop {
        let multipart = recv_sock.recv().await?;
        info!(
            "Request: {:?}",
            multipart
                .iter()
                .map(|item| item.as_str().unwrap_or("invalid text"))
                .collect::<Vec<&str>>()
        );
        recv_sock.send(multipart).await?;
    }
}
