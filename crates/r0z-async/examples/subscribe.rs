use futures::StreamExt;

use r0z_async::{subscribe, Context, Result};

use log::info;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = env::var("RUST_LOG").unwrap_or_else(|_| "subscribe=DEBUG".to_owned());
    pretty_env_logger::formatted_builder()
        .parse_filters(&filter)
        .init();

    let mut socket = subscribe(&Context::new())
        .connect("tcp://127.0.0.1:7899")?
        .subscribe(b"topic")?;

    while let Some(msg) = socket.next().await {
        info!(
            "Subscribe: {:?}",
            msg?.iter()
                .map(|item| item.as_str().unwrap_or("invalid text"))
                .collect::<Vec<&str>>()
        );
    }
    Ok(())
}
