use r0rz::Context as ZmqContext;

use crate::{comm::Receiver, poll::ZmqPoller, FromZmqSocket, SocketBuilder};

/// Create a builder for a PULL socket.
pub fn pull(context: &ZmqContext) -> SocketBuilder<Pull> {
    SocketBuilder::new(context, r0rz::SocketType::PULL)
}

/// Asynchronous PULL socket.
pub struct Pull {
    inner: Receiver,
}

impl FromZmqSocket<Pull> for Pull {
    fn from_zmq_socket(
        socket: r0rz::Socket,
        register: crate::runtime::Register,
    ) -> crate::Result<Self> {
        Ok(Self {
            inner: Receiver::new(ZmqPoller::from_zmq_socket(socket, register)?),
        })
    }
}

impl_wrapper!(Pull, Receiver, inner);
impl_wrapper_stream!(Pull, inner);
impl_buffered!(Pull, inner);
