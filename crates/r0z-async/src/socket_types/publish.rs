use r0z::Context as ZmqContext;

use crate::{poll::ZmqPoller, FromZmqSocket, Sender, SocketBuilder};

/// Create a builder for a PUB socket.
///
/// ## Usage Example
///
/// ```rust,no_run
/// # #[cfg(feature = "tokio")]
/// # mod example {
/// use r0z_async::{publish, Context, Result};
///
/// use futures::SinkExt;
/// use log::info;
/// use std::env;
/// use std::time::Duration;
/// use tokio::time::sleep;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///
///     let mut socket = publish(&Context::new()).bind("tcp://127.0.0.1:7899")?;
///
///     let mut i = 0;
///
///     loop {
///         i += 1;
///
///         socket
///             .send(vec!["topic", &format!("Broadcast #{}", i)])
///             .await?;
///
///         sleep(Duration::from_secs(1)).await;
///     }
/// }
/// # }
/// # fn main() {}
/// ```
pub fn publish(context: &ZmqContext) -> SocketBuilder<Publish> {
    SocketBuilder::new(context, r0z::SocketType::PUB)
}

/// Asynchronous PUB socket.
pub struct Publish {
    inner: Sender,
}

impl FromZmqSocket<Publish> for Publish {
    fn from_zmq_socket(
        socket: r0z::Socket,
        register: crate::runtime::Register,
    ) -> crate::Result<Self> {
        Ok(Self {
            inner: Sender::new(ZmqPoller::from_zmq_socket(socket, register)?),
        })
    }
}

impl_wrapper!(Publish, Sender, inner);
impl_wrapper_sink!(Publish, inner);
