use crate::runtime::{DefaultRuntime, Runtime};
use r0z::Context as ZmqContext;

use crate::{FromZmqSocket, SocketBuilder, comm::SenderReceiver, poll::ZmqPoller};

/// Create a builder for a DEALER socket.
pub fn dealer(context: &ZmqContext) -> SocketBuilder<Dealer> {
    SocketBuilder::new(context, r0z::SocketType::DEALER)
}

/// Asynchronous DEALER Socket.
pub struct Dealer<R: Runtime = DefaultRuntime> {
    inner: SenderReceiver<R>,
}

impl<R: Runtime> FromZmqSocket for Dealer<R> {
    type WithRuntime<S: Runtime> = Dealer<S>;
    fn from_zmq_socket(socket: r0z::Socket) -> crate::Result<Self> {
        Ok(Self {
            inner: SenderReceiver::new(ZmqPoller::from_zmq_socket(socket)?),
        })
    }
}

impl_wrapper!(Dealer, SenderReceiver, inner);
impl_wrapper_sink!(Dealer, inner);
impl_wrapper_stream!(Dealer, inner);
