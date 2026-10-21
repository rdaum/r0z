use r0z_async::{publish, Context, Result};

use futures::SinkExt;
use log::info;
use std::env;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = env::var("RUST_LOG").unwrap_or_else(|_| "publish=DEBUG".to_owned());
    pretty_env_logger::formatted_builder()
        .parse_filters(&filter)
        .init();

    let mut socket = publish(&Context::new()).bind("tcp://127.0.0.1:7899")?;

    let mut i = 0;
    loop {
        i += 1;
        let message = format!("Broadcast #{}", i);
        info!("Publish: {}", message);

        socket
            .send(vec![b"topic" as &[u8], message.as_bytes()])
            .await?;
        sleep(Duration::from_secs(1)).await;
    }
}
