use std::pin::Pin;

use crate::{poll::ZmqPoller, FromZmqSocket, Multipart, SocketBuilder, TmqError};
use r0rz::Context as ZmqContext;

/// Creates a REQ socket builder. The socket must send before it can receive.
pub fn request(context: &ZmqContext) -> SocketBuilder<RequestReply> {
    SocketBuilder::new(context, r0rz::SocketType::REQ)
}

/// Creates a REP socket builder. The socket must receive before it can send.
pub fn reply(context: &ZmqContext) -> SocketBuilder<RequestReply> {
    SocketBuilder::new(context, r0rz::SocketType::REP)
}

/// The next permitted operation on a [`RequestReply`] socket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestReplyState {
    /// The socket can accept a new multipart message.
    SendReady,
    /// A send is buffered. Call [`RequestReply::flush`] to finish it.
    SendPending,
    /// The socket can receive its next multipart message.
    ReceiveReady,
    /// An I/O error left progress uncertain. Recreate the socket before further I/O.
    Failed,
}

/// A REQ or REP socket with borrowing send and receive operations.
///
/// Operations retain caller ownership on cancellation and errors. Successful sends and receives
/// alternate, as required by native ZeroMQ. Check [`Self::state`] after cancellation.
///
/// Cancelling a pending receive leaves it available for another `recv`. Cancelling a pending send
/// retains its multipart in the socket; call `flush` to resume it. A completed send cannot be undone.
/// A REQ socket must receive the outstanding reply before it can send another request.
///
/// Native or reactor errors mark the socket as `Failed`, because multipart progress can be uncertain.
/// This includes errors after a partial send. Recreate failed sockets; they remain available for
/// inspection and linger configuration. Invalid operations and empty messages do not change state.
///
/// Use [`crate::AsZmqSocket::get_socket`] for inspection and options only. Native send/receive calls and
/// relaxed REQ sequencing bypass the state tracked by this wrapper and are not supported.
pub struct RequestReply {
    inner: ZmqPoller,
    state: RequestReplyState,
    pending: Multipart,
}

impl FromZmqSocket<RequestReply> for RequestReply {
    fn from_zmq_socket(
        socket: r0rz::Socket,
        register: crate::runtime::Register,
    ) -> crate::Result<Self> {
        let state = match socket.get_socket_type()? {
            r0rz::REQ => RequestReplyState::SendReady,
            r0rz::REP => RequestReplyState::ReceiveReady,
            _ => return Err(r0rz::Error::EINVAL.into()),
        };
        Ok(Self {
            inner: ZmqPoller::from_zmq_socket(socket, register)?,
            state,
            pending: Multipart::default(),
        })
    }
}

impl_as_socket!(RequestReply, inner);

impl RequestReply {
    /// Returns the current protocol state, including a buffered send or a failed operation.
    pub fn state(&self) -> RequestReplyState {
        self.state
    }

    fn require(&self, expected: RequestReplyState, operation: &'static str) -> crate::Result<()> {
        if self.state == expected {
            Ok(())
        } else {
            Err(TmqError::InvalidRequestReplyState {
                operation,
                state: self.state,
            })
        }
    }

    /// Sends one multipart message without consuming the socket.
    ///
    /// The first poll stores the message in the socket. If cancelled while pending, use `flush`
    /// to resume that message. Another `send` returns an error and cannot replace the buffered data.
    /// Dropping this future before its first poll leaves the socket unchanged and drops the message.
    /// An empty multipart returns `EINVAL`; a multipart containing one empty frame is valid.
    pub async fn send(&mut self, msg: Multipart) -> crate::Result<()> {
        self.require(RequestReplyState::SendReady, "send")?;
        if msg.is_empty() {
            return Err(r0rz::Error::EINVAL.into());
        }
        self.pending = msg;
        self.state = RequestReplyState::SendPending;
        self.flush().await
    }

    /// Resumes a buffered send. This is a no-op when no send is buffered, unless the socket failed.
    ///
    /// Cancellation retains the buffered message. Success changes the state to `ReceiveReady`.
    pub async fn flush(&mut self) -> crate::Result<()> {
        match self.state {
            RequestReplyState::SendReady | RequestReplyState::ReceiveReady => return Ok(()),
            RequestReplyState::Failed => {
                return self.require(RequestReplyState::SendPending, "flush");
            }
            RequestReplyState::SendPending => {}
        }
        let result = futures::future::poll_fn(|cx| {
            Pin::new(&mut self.inner).multipart_flush(cx, &mut self.pending)
        })
        .await;
        self.state = if result.is_ok() {
            RequestReplyState::ReceiveReady
        } else {
            RequestReplyState::Failed
        };
        result
    }

    /// Receives one multipart message without consuming the socket.
    ///
    /// Cancellation while pending leaves the state as `ReceiveReady`. Call `recv` again to resume.
    /// Success changes the state to `SendReady`. A timeout does not reset the native protocol.
    pub async fn recv(&mut self) -> crate::Result<Multipart> {
        self.require(RequestReplyState::ReceiveReady, "recv")?;
        let result =
            futures::future::poll_fn(|cx| Pin::new(&mut self.inner).multipart_recv(cx)).await;
        self.state = if result.is_ok() {
            RequestReplyState::SendReady
        } else {
            RequestReplyState::Failed
        };
        result
    }
}
