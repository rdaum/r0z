//! Runtime adapters for ZeroMQ notification descriptors.
//!
//! Select an adapter with [`crate::SocketBuilder::with_runtime`]. Tokio is the default when its
//! feature is enabled; otherwise `async-io` is the default when available. With neither feature,
//! supply a custom adapter. Built-in adapters currently support Unix only.
//!
//! Each socket owns one boxed registration. Socket types and multipart logic remain independent
//! of the adapter, so different adapters can coexist without making the socket API generic.

use std::{
    io,
    task::{Context, Poll},
};

/// An owned socket registered with a runtime's readiness driver.
///
/// Implementations must keep the native socket alive until after deregistration. The descriptor
/// returned by `ZMQ_FD` belongs to libzmq: never read, write, or close it directly, or change its
/// flags. Poll only for readable notifications, even when waiting to send a ZeroMQ message.
///
/// The shared core supplies one waker that forwards notifications to both logical directions.
/// Implementations need only retain the most recent supplied waker.
pub trait Registration: Send {
    /// Returns the registered native socket.
    fn socket(&self) -> &r0z::Socket;

    /// Polls for and acknowledges a readable notification.
    ///
    /// `Pending` must register the supplied waker. `Ready(Ok(()))` consumes the observed readiness
    /// so a subsequent poll can wait for another notification. Do not consume a newer event while
    /// acknowledging an older one. Spurious notifications are allowed; the core checks `ZMQ_EVENTS`.
    fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
}

/// Registers native sockets with a readiness driver.
pub trait Runtime {
    /// Registers a socket and transfers ownership to the returned registration.
    fn register(socket: r0z::Socket) -> crate::Result<Box<dyn Registration>>;
}

#[doc(hidden)]
pub type Register = fn(r0z::Socket) -> crate::Result<Box<dyn Registration>>;

pub(crate) fn register_default(socket: r0z::Socket) -> crate::Result<Box<dyn Registration>> {
    #[cfg(all(unix, feature = "tokio"))]
    {
        Tokio::register(socket)
    }
    #[cfg(all(unix, not(feature = "tokio"), feature = "async-io"))]
    {
        AsyncIo::register(socket)
    }
    #[cfg(not(all(unix, any(feature = "tokio", feature = "async-io"))))]
    {
        drop(socket);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no default runtime adapter; select one with SocketBuilder::with_runtime",
        )
        .into())
    }
}

#[cfg(all(unix, any(feature = "tokio", feature = "async-io")))]
struct SocketSource {
    socket: r0z::Socket,
    fd: std::os::fd::RawFd,
}

#[cfg(all(unix, any(feature = "tokio", feature = "async-io")))]
impl SocketSource {
    fn new(socket: r0z::Socket) -> crate::Result<Self> {
        Ok(Self {
            fd: socket.get_fd()?,
            socket,
        })
    }
}

#[cfg(all(unix, any(feature = "tokio", feature = "async-io")))]
impl std::os::fd::AsRawFd for SocketSource {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.fd
    }
}

#[cfg(all(unix, feature = "async-io"))]
impl std::os::fd::AsFd for SocketSource {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        // SAFETY: this source owns the socket, which keeps its notification descriptor alive.
        // The returned borrow cannot outlive the source and never assumes ownership of the fd.
        unsafe { std::os::fd::BorrowedFd::borrow_raw(self.fd) }
    }
}

/// Tokio readiness adapter. Construct sockets inside a Tokio runtime with I/O enabled.
#[cfg(all(unix, feature = "tokio"))]
pub struct Tokio;

#[cfg(all(unix, feature = "tokio"))]
struct TokioRegistration(tokio::io::unix::AsyncFd<SocketSource>);

#[cfg(all(unix, feature = "tokio"))]
impl Runtime for Tokio {
    fn register(socket: r0z::Socket) -> crate::Result<Box<dyn Registration>> {
        Ok(Box::new(TokioRegistration(
            tokio::io::unix::AsyncFd::with_interest(
                SocketSource::new(socket)?,
                tokio::io::Interest::READABLE,
            )?,
        )))
    }
}

#[cfg(all(unix, feature = "tokio"))]
impl Registration for TokioRegistration {
    fn socket(&self) -> &r0z::Socket {
        &self.0.get_ref().socket
    }

    fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut ready = futures::ready!(self.0.poll_read_ready(cx))?;
        ready.clear_ready();
        Poll::Ready(Ok(()))
    }
}

/// The `async-io` readiness adapter, usable with smol and other executors.
#[cfg(all(unix, feature = "async-io"))]
pub struct AsyncIo;

#[cfg(all(unix, feature = "async-io"))]
struct AsyncIoRegistration(async_io::Async<SocketSource>);

#[cfg(all(unix, feature = "async-io"))]
impl Runtime for AsyncIo {
    fn register(socket: r0z::Socket) -> crate::Result<Box<dyn Registration>> {
        // Register without changing flags on libzmq's descriptor. All actual I/O uses libzmq
        // with DONTWAIT; we never read or write the notification descriptor itself.
        Ok(Box::new(AsyncIoRegistration(
            async_io::Async::new_nonblocking(SocketSource::new(socket)?)?,
        )))
    }
}

#[cfg(all(unix, feature = "async-io"))]
impl Registration for AsyncIoRegistration {
    fn socket(&self) -> &r0z::Socket {
        &self.0.get_ref().socket
    }

    fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.poll_readable(cx)
    }
}
