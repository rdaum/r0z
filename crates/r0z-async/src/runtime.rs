//! Runtime adapters for ZeroMQ notification descriptors.
//!
//! Select an adapter with [`crate::SocketBuilder::with_runtime`]. Tokio is the default when its
//! feature is enabled; otherwise `async-io`, then compio, is the default when available.
//! Without a runtime feature, supply a custom adapter. Built-in adapters currently support Unix only.
//!
//! Socket types carry their runtime adapter as a type parameter. The registration determines
//! whether a socket can move between threads. Compio sockets remain on their runtime thread.

use std::{
    io,
    task::{Context, Poll},
};

/// An owned socket registered with a runtime's readiness driver.
///
/// Implementations must keep each polled descriptor alive until after deregistration. The descriptor
/// returned by `ZMQ_FD` belongs to libzmq: never read, write, or close it directly, or change its
/// flags. Poll only for readable notifications, even when waiting to send a ZeroMQ message.
///
/// The shared core supplies one waker that forwards notifications to both logical directions.
/// Implementations need only retain the most recent supplied waker.
pub trait Registration: Unpin {
    /// Returns the registered native socket.
    fn socket(&self) -> &r0z::Socket;

    /// Checks the caller's runtime and cancellation context before native I/O.
    ///
    /// `direction` is either `r0z::POLLIN` or `r0z::POLLOUT`. The original task context is
    /// supplied here, before the core replaces its waker for shared readiness notifications.
    /// Use `wake` for cancellation notifications; it forwards to the currently waiting tasks
    /// without retaining a task after its operation completes. Track tokens separately for each
    /// direction. An error prevents this poll from consuming or sending frames.
    /// The default imposes no restrictions.
    fn check_context(
        &self,
        _cx: &mut Context<'_>,
        _direction: r0z::PollEvents,
        _wake: &std::task::Waker,
    ) -> io::Result<()> {
        Ok(())
    }

    /// Polls for and acknowledges a readable notification.
    ///
    /// `Pending` must register the supplied waker. `Ready(Ok(()))` consumes the observed readiness
    /// so a subsequent poll can wait for another notification. Do not consume a newer event while
    /// acknowledging an older one. Spurious notifications are allowed; the core checks `ZMQ_EVENTS`.
    fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
}

/// Registers native sockets with a readiness driver.
pub trait Runtime {
    /// The owned registration, including any runtime-specific thread restrictions.
    type Registration: Registration;

    /// Registers a socket and transfers ownership to the returned registration.
    fn register(socket: r0z::Socket) -> crate::Result<Self::Registration>;
}

/// Default adapter: Tokio, then async-io, then compio, in feature priority order.
#[cfg(all(unix, feature = "tokio"))]
pub type DefaultRuntime = Tokio;
/// Default adapter when Tokio is disabled.
#[cfg(all(unix, not(feature = "tokio"), feature = "async-io"))]
pub type DefaultRuntime = AsyncIo;
/// Default adapter when only compio is enabled.
#[cfg(all(
    unix,
    not(any(feature = "tokio", feature = "async-io")),
    feature = "compio"
))]
pub type DefaultRuntime = Compio;
/// No built-in adapter is enabled. Select a custom runtime through the socket builder.
#[cfg(not(all(unix, any(feature = "tokio", feature = "async-io", feature = "compio"))))]
pub type DefaultRuntime = NoRuntime;

/// Placeholder for builds without a default runtime adapter.
pub enum NoRuntime {}

impl Runtime for NoRuntime {
    type Registration = Self;

    fn register(socket: r0z::Socket) -> crate::Result<Self> {
        drop(socket);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no default runtime adapter; select one with SocketBuilder::with_runtime",
        )
        .into())
    }
}

impl Registration for NoRuntime {
    fn socket(&self) -> &r0z::Socket {
        match *self {}
    }
    fn poll_readable(&self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        match *self {}
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
///
/// Registration returns an I/O error outside a runtime. Tokio panics if the current runtime
/// has its I/O driver disabled.
#[cfg(all(unix, feature = "tokio"))]
pub struct Tokio;

/// Owned Tokio registration. Created by [`Tokio`].
#[cfg(all(unix, feature = "tokio"))]
pub struct TokioRegistration(tokio::io::unix::AsyncFd<SocketSource>);

#[cfg(all(unix, feature = "tokio"))]
impl Runtime for Tokio {
    type Registration = TokioRegistration;

    fn register(socket: r0z::Socket) -> crate::Result<Self::Registration> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("construct Tokio sockets inside a Tokio runtime"))?;
        let source = SocketSource::new(socket)?;
        // SAFETY: source owns the socket and always returns the same notification fd.
        // TokioRegistration never replaces the source or exposes mutable access to it.
        // AsyncFd deregisters before dropping the source, which keeps the fd open until then.
        let registered = unsafe {
            tokio::io::unix::AsyncFd::register_with_interest(source, tokio::io::Interest::READABLE)
        }
        .map_err(io::Error::from)?;
        Ok(TokioRegistration(registered))
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

/// Owned async-io registration. Created by [`AsyncIo`].
#[cfg(all(unix, feature = "async-io"))]
pub struct AsyncIoRegistration(async_io::Async<SocketSource>);

#[cfg(all(unix, feature = "async-io"))]
impl Runtime for AsyncIo {
    type Registration = AsyncIoRegistration;

    fn register(socket: r0z::Socket) -> crate::Result<Self::Registration> {
        // Register without changing flags on libzmq's descriptor. All actual I/O uses libzmq
        // with DONTWAIT; we never read or write the notification descriptor itself.
        Ok(AsyncIoRegistration(async_io::Async::new_nonblocking(
            SocketSource::new(socket)?,
        )?))
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

/// Compio readiness adapter. Create and use sockets inside a compio runtime on one thread.
///
/// These sockets are not `Send` or `Sync`. Split halves can run in separate local tasks.
/// Polling outside the runtime that registered the socket returns an I/O error.
/// Compio cancellation tokens return `ECANCELED` independently for each direction.
///
/// ```
/// use r0z_async::{dealer::Dealer, runtime::Compio};
/// fn require_unpin<T: Unpin>() {}
/// require_unpin::<Dealer<Compio>>();
/// ```
///
/// ```compile_fail
/// use r0z_async::{dealer::Dealer, runtime::Compio};
/// fn require_send<T: Send>() {}
/// require_send::<Dealer<Compio>>();
/// ```
///
/// ```compile_fail
/// use r0z_async::{dealer::Dealer, runtime::Compio};
/// fn require_sync<T: Sync>() {}
/// require_sync::<Dealer<Compio>>();
/// ```
#[cfg(all(unix, feature = "compio"))]
pub struct Compio;

/// Owned compio registration. Created by [`Compio`].
#[cfg(all(unix, feature = "compio"))]
pub struct CompioRegistration {
    // Drop the pending wait before closing the socket. Driver cancellation can retain only
    // our duplicate, never the native socket or its context.
    notification: compio::runtime::fd::PollFd<std::os::fd::OwnedFd>,
    socket: r0z::Socket,
    runtime_waker: std::task::Waker,
    read_cancel: std::cell::RefCell<Option<CompioCancellation>>,
    write_cancel: std::cell::RefCell<Option<CompioCancellation>>,
}

#[cfg(all(unix, feature = "compio"))]
struct CompioCancellation {
    token: compio::runtime::CancelToken,
    wait: std::pin::Pin<Box<dyn Future<Output = ()>>>,
}

#[cfg(all(unix, feature = "compio"))]
impl CompioRegistration {
    fn check_runtime(&self) -> io::Result<()> {
        // Compio's driver returns wakers backed by its shared notifier. Retaining that
        // notifier identifies the driver without retaining the runtime's task executor.
        // A Runtime clone here would create a cycle for tasks that own sockets.
        if matches!(
            compio::runtime::Runtime::try_with_current(|runtime| {
                self.runtime_waker.will_wake(&runtime.waker())
            }),
            Ok(true)
        ) {
            Ok(())
        } else {
            Err(io::Error::other(
                "poll compio sockets inside their original runtime",
            ))
        }
    }
}

#[cfg(all(unix, feature = "compio"))]
impl Runtime for Compio {
    type Registration = CompioRegistration;

    fn register(socket: r0z::Socket) -> crate::Result<Self::Registration> {
        let runtime_waker = compio::runtime::Runtime::try_with_current(|runtime| runtime.waker())
            .map_err(|_| {
            io::Error::other("construct compio sockets inside a compio runtime")
        })?;
        let fd = socket.get_fd()?;
        // SAFETY: socket owns this open notification descriptor throughout the borrow and
        // duplication. Only the duplicate becomes owned; libzmq retains its original fd.
        let notification =
            unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) }.try_clone_to_owned()?;
        Ok(CompioRegistration {
            notification: compio::runtime::fd::PollFd::new(notification)?,
            socket,
            runtime_waker,
            read_cancel: Default::default(),
            write_cancel: Default::default(),
        })
    }
}

#[cfg(all(unix, feature = "compio"))]
impl Registration for CompioRegistration {
    fn socket(&self) -> &r0z::Socket {
        &self.socket
    }

    fn check_context(
        &self,
        cx: &mut Context<'_>,
        direction: r0z::PollEvents,
        wake: &std::task::Waker,
    ) -> io::Result<()> {
        self.check_runtime()?;
        // Read the token before the core substitutes its shared notification waker. Keep
        // cancellation separate from PollFd: one direction must not cancel the other's wait,
        // and a dropped future's token must not cancel a later operation on this socket.
        let Poll::Ready(token) = std::pin::pin!(compio::runtime::CancelToken::current()).poll(cx)
        else {
            unreachable!("CancelToken::current completes immediately")
        };
        let slot = if direction == r0z::POLLIN {
            &self.read_cancel
        } else {
            &self.write_cancel
        };
        let mut cancellation = slot.borrow_mut();
        if cancellation.as_ref().map(|c| &c.token) != token.as_ref() {
            *cancellation = token.map(|token| CompioCancellation {
                wait: Box::pin(token.clone().wait()),
                token,
            });
        }
        // Retain only the forwarding waker, not the caller or its ExtWaker token data.
        if let Some(cancellation) = cancellation.as_mut()
            && cancellation
                .wait
                .as_mut()
                .poll(&mut Context::from_waker(wake))
                .is_ready()
        {
            return Err(io::Error::from_raw_os_error(libc::ECANCELED));
        }
        Ok(())
    }

    fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check_runtime()?;
        // Completing PollOnce consumes this notification. The next poll submits a new wait.
        self.notification.poll_read_ready(cx)
    }
}
