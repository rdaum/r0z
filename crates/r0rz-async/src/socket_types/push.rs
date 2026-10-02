use r0rz::Context as ZmqContext;

use crate::{comm::Sender, poll::ZmqPoller, FromZmqSocket, SocketBuilder};

/// Create a builder for a PUSH socket.
pub fn push(context: &ZmqContext) -> SocketBuilder<Push> {
    SocketBuilder::new(context, r0rz::SocketType::PUSH)
}

/// Asynchronous PUSH socket.
pub struct Push {
    inner: Sender,
}

impl FromZmqSocket<Push> for Push {
    fn from_zmq_socket(socket: r0rz::Socket) -> crate::Result<Self> {
        Ok(Self {
            inner: Sender::new(ZmqPoller::from_zmq_socket(socket)?),
        })
    }
}

impl_wrapper!(Push, Sender, inner);
impl_wrapper_sink!(Push, inner);
