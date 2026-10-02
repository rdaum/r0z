use r0z::Context as ZmqContext;

use crate::{comm::SenderReceiver, poll::ZmqPoller, FromZmqSocket, SocketBuilder};

/// Create a builder for a DEALER socket.
pub fn dealer(context: &ZmqContext) -> SocketBuilder<Dealer> {
    SocketBuilder::new(context, r0z::SocketType::DEALER)
}

/// Asynchronous DEALER Socket.
pub struct Dealer {
    inner: SenderReceiver,
}

impl FromZmqSocket<Dealer> for Dealer {
    fn from_zmq_socket(
        socket: r0z::Socket,
        register: crate::runtime::Register,
    ) -> crate::Result<Self> {
        Ok(Self {
            inner: SenderReceiver::new(ZmqPoller::from_zmq_socket(socket, register)?),
        })
    }
}

impl_wrapper!(Dealer, SenderReceiver, inner);
impl_wrapper_sink!(Dealer, inner);
impl_wrapper_stream!(Dealer, inner);
