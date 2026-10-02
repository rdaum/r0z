use thiserror::Error;

/// Error that can occur during an async ZMQ operation.
#[derive(Error, Debug)]
pub enum TmqError {
    /// Inner ZMQ error.
    #[error("Zmq error: {0}")]
    Zmq(#[from] r0z::Error),
    /// A send operation of a multipart message was successfully started, but it could not be finished.
    #[error(
        "Interrupted Zmq send. Please report this at https://github.com/rdaum/rust-zmq/issues"
    )]
    InterruptedSend,
    /// An operation does not match the tracked REQ/REP protocol state.
    #[error("Cannot {operation} a request/reply socket in state {state:?}")]
    InvalidRequestReplyState {
        /// The rejected operation.
        operation: &'static str,
        /// The unchanged socket state.
        state: crate::request_reply::RequestReplyState,
    },
    /// General IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
