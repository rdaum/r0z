use crate::runtime::{DefaultRuntime, Runtime};
use r0z::Context as ZmqContext;

use crate::{FromZmqSocket, SocketBuilder, comm::Receiver, poll::ZmqPoller};

/// Create a builder for a PULL socket.
pub fn pull(context: &ZmqContext) -> SocketBuilder<Pull> {
    SocketBuilder::new(context, r0z::SocketType::PULL)
}

/// Asynchronous PULL socket.
pub struct Pull<R: Runtime = DefaultRuntime> {
    inner: Receiver<R>,
}

impl<R: Runtime> FromZmqSocket for Pull<R> {
    type WithRuntime<S: Runtime> = Pull<S>;
    fn from_zmq_socket(socket: r0z::Socket) -> crate::Result<Self> {
        Ok(Self {
            inner: Receiver::new(ZmqPoller::from_zmq_socket(socket)?),
        })
    }
}

impl_wrapper!(Pull, Receiver, inner);
impl_wrapper_stream!(Pull, inner);
impl_buffered!(Pull, inner);
