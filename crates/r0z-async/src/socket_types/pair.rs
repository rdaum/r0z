use crate::runtime::{DefaultRuntime, Runtime};
use r0z::Context as ZmqContext;

use crate::{FromZmqSocket, SocketBuilder, comm::SenderReceiver, poll::ZmqPoller};

/// Create a builder for a PAIR socket.
pub fn pair(context: &ZmqContext) -> SocketBuilder<Pair> {
    SocketBuilder::new(context, r0z::SocketType::PAIR)
}

/// Asynchronous PAIR Socket.
pub struct Pair<R: Runtime = DefaultRuntime> {
    inner: SenderReceiver<R>,
}

impl<R: Runtime> FromZmqSocket for Pair<R> {
    type WithRuntime<S: Runtime> = Pair<S>;
    fn from_zmq_socket(socket: r0z::Socket) -> crate::Result<Self> {
        Ok(Self {
            inner: SenderReceiver::new(ZmqPoller::from_zmq_socket(socket)?),
        })
    }
}

impl_wrapper!(Pair, SenderReceiver, inner);
impl_wrapper_sink!(Pair, inner);
impl_wrapper_stream!(Pair, inner);
