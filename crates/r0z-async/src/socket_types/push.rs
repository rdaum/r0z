use r0z::Context as ZmqContext;

use crate::{FromZmqSocket, SocketBuilder, comm::Sender, poll::ZmqPoller};

/// Create a builder for a PUSH socket.
pub fn push(context: &ZmqContext) -> SocketBuilder<Push> {
    SocketBuilder::new(context, r0z::SocketType::PUSH)
}

/// Asynchronous PUSH socket.
pub struct Push {
    inner: Sender,
}

impl FromZmqSocket<Push> for Push {
    fn from_zmq_socket(
        socket: r0z::Socket,
        register: crate::runtime::Register,
    ) -> crate::Result<Self> {
        Ok(Self {
            inner: Sender::new(ZmqPoller::from_zmq_socket(socket, register)?),
        })
    }
}

impl_wrapper!(Push, Sender, inner);
impl_wrapper_sink!(Push, inner);
