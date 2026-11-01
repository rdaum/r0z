use crate::runtime::{DefaultRuntime, Runtime};
use r0z::Context as ZmqContext;

use crate::{FromZmqSocket, Receiver, SocketBuilder, poll::ZmqPoller, socket::AsZmqSocket};

/// Create a builder for a SUB socket.
///
/// ## Usage Example
///
/// ```rust,no_run
/// # #[cfg(feature = "tokio")]
/// # mod example {
/// use futures::StreamExt;
///
/// use r0z_async::{subscribe, Context, Result};
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///
///     let mut socket = subscribe(&Context::new())
///         .connect("tcp://127.0.0.1:7899")?
///         .subscribe(b"topic")?;
///
///     while let Some(msg) = socket.next().await {
///         println!(
///             "Subscribe: {:?}",
///             msg?.iter()
///                 .map(|item| item.as_str().unwrap_or("invalid text"))
///                 .collect::<Vec<&str>>()
///         );
///     }
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
pub fn subscribe(context: &ZmqContext) -> SocketBuilder<SubscribeWithoutTopic> {
    SocketBuilder::new(context, r0z::SocketType::SUB)
}

/// SUB socket which is already bound or connected, but isn't yet subscribed to a topic.
pub struct SubscribeWithoutTopic<R: Runtime = DefaultRuntime> {
    inner: ZmqPoller<R>,
}

impl<R: Runtime> FromZmqSocket for SubscribeWithoutTopic<R> {
    type WithRuntime<S: Runtime> = SubscribeWithoutTopic<S>;
    fn from_zmq_socket(socket: r0z::Socket) -> crate::Result<Self> {
        Ok(Self {
            inner: ZmqPoller::from_zmq_socket(socket)?,
        })
    }
}

impl<R: Runtime> SubscribeWithoutTopic<R> {
    /// Finishes creating the SUB socket by subscribing to the given topic.
    pub fn subscribe(self, topic: &[u8]) -> crate::Result<Subscribe<R>> {
        self.inner.get_socket().set_subscribe(topic)?;
        Ok(Subscribe {
            inner: Receiver::new(self.inner),
        })
    }
}

/// Asynchronous SUB socket.
pub struct Subscribe<R: Runtime = DefaultRuntime> {
    inner: Receiver<R>,
}
impl_wrapper!(Subscribe, Receiver, inner);
impl_wrapper_stream!(Subscribe, inner);

impl<R: Runtime> Subscribe<R> {
    /// Adds another topic to this subscriber.
    /// This doesn't remove the previously added topics.
    pub fn subscribe(&mut self, topic: &[u8]) -> crate::Result<()> {
        self.get_socket().set_subscribe(topic)?;
        Ok(())
    }

    /// Removes a topic from this subscriber.
    pub fn unsubscribe(&mut self, topic: &[u8]) -> crate::Result<()> {
        self.get_socket().set_unsubscribe(topic)?;
        Ok(())
    }
}
