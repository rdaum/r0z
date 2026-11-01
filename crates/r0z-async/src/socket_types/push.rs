use crate::runtime::{DefaultRuntime, Runtime};
use r0z::Context as ZmqContext;

use crate::{FromZmqSocket, SocketBuilder, comm::Sender, poll::ZmqPoller};

/// Create a builder for a PUSH socket.
pub fn push(context: &ZmqContext) -> SocketBuilder<Push> {
    SocketBuilder::new(context, r0z::SocketType::PUSH)
}

/// Asynchronous PUSH socket.
pub struct Push<R: Runtime = DefaultRuntime> {
    inner: Sender<R>,
}

impl<R: Runtime> FromZmqSocket for Push<R> {
    type WithRuntime<S: Runtime> = Push<S>;
    fn from_zmq_socket(socket: r0z::Socket) -> crate::Result<Self> {
        Ok(Self {
            inner: Sender::new(ZmqPoller::from_zmq_socket(socket)?),
        })
    }
}

impl_wrapper!(Push, Sender, inner);
impl_wrapper_sink!(Push, inner);
