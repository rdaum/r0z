use futures::ready;

use crate::error::TmqError::InterruptedSend;
use crate::runtime::{Register, Registration};
use crate::socket::AsZmqSocket;
use crate::{Multipart, Result};
use futures::task::{waker_ref, ArcWake, AtomicWaker};
use r0z::Socket;
use std::sync::Arc;
use std::{
    collections::VecDeque,
    task::{Context, Poll},
};

/// Implements functions for asynchronous reading and writing of multipart messages.
pub(crate) struct ZmqPoller {
    registration: Box<dyn Registration>,
    waiters: Arc<Waiters>,
}

#[derive(Default)]
struct Waiters {
    read: AtomicWaker,
    write: AtomicWaker,
}

impl ArcWake for Waiters {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.read.wake();
        arc_self.write.wake();
    }
}

impl ZmqPoller {
    pub(crate) fn from_zmq_socket(socket: r0z::Socket, register: Register) -> Result<Self> {
        Ok(Self {
            registration: register(socket)?,
            waiters: Arc::default(),
        })
    }

    // Native I/O can change the opposite direction without another descriptor edge.
    fn after_io(&self, direction: r0z::PollEvents) -> Result<()> {
        let events = self.get_socket().get_events()?;
        self.wake_opposite(events, direction);
        Ok(())
    }

    fn wake_opposite(&self, events: r0z::PollEvents, direction: r0z::PollEvents) {
        if direction == r0z::POLLOUT && events.contains(r0z::POLLIN) {
            self.waiters.read.wake();
        }
        if direction == r0z::POLLIN && events.contains(r0z::POLLOUT) {
            self.waiters.write.wake();
        }
    }
}

impl AsZmqSocket for ZmqPoller {
    fn get_socket(&self) -> &Socket {
        self.registration.socket()
    }
}

impl ZmqPoller {
    /// Attempt to receive a Multipart message from a ZeroMQ socket with buffering.
    ///
    /// If there is any message in the buffer, it will be returned right away.
    /// If not, a batch of messages up to the capacity of the buffer will be read from the socket.
    ///
    /// If nothing was received, the read flag is cleared.
    pub(crate) fn multipart_recv_buffered(
        &self,
        cx: &mut Context<'_>,
        read_buffer: &mut ReceiverBuffer,
    ) -> Poll<Result<Multipart>> {
        if read_buffer.is_empty() {
            ready!(self.multipart_poll_read_ready(cx))?;

            let mut buffer = Multipart::default();
            loop {
                let mut msg = r0z::Message::new();
                let result = self.get_socket().recv(&mut msg, r0z::DONTWAIT);
                self.after_io(r0z::POLLIN)?;
                match result {
                    Ok(_) => {
                        let more = msg.get_more();
                        buffer.push_back(msg);
                        if !more {
                            read_buffer.push_back(buffer);
                            if read_buffer.is_full() {
                                break Poll::Ready(Ok(read_buffer.pop_front().unwrap()));
                            }

                            buffer = Multipart::default();
                        }
                    }
                    Err(r0z::Error::EAGAIN) => {
                        if !buffer.is_empty() {
                            read_buffer.push_back(buffer);
                        }
                        self.rearm(cx, r0z::POLLIN)?;

                        if read_buffer.is_empty() {
                            break Poll::Pending;
                        } else {
                            break Poll::Ready(Ok(read_buffer.pop_front().unwrap()));
                        }
                    }
                    Err(e) => break Poll::Ready(Err(e.into())),
                }
            }
        } else {
            Poll::Ready(Ok(read_buffer.pop_front().unwrap()))
        }
    }

    /// Attempt to receive a Multipart message from a ZeroMQ socket.
    ///
    /// Either the whole multipart at once or nothing is received (according to
    /// [http://zguide.zeromq.org/page:all#Multipart-Messages], when one part of a multipart message
    /// has been received, all the others are already available as well).
    ///
    /// If nothing was received, the read flag is cleared.
    pub(crate) fn multipart_recv(&self, cx: &mut Context<'_>) -> Poll<Result<Multipart>> {
        ready!(self.multipart_poll_read_ready(cx))?;

        let mut buffer = Multipart::default();
        loop {
            let mut msg = r0z::Message::new();
            let result = self.get_socket().recv(&mut msg, r0z::DONTWAIT);
            self.after_io(r0z::POLLIN)?;
            match result {
                Ok(_) => {
                    let more = msg.get_more();
                    buffer.push_back(msg);
                    if !more {
                        break;
                    }
                }
                Err(r0z::Error::EAGAIN) => {
                    assert!(buffer.is_empty());
                    log::warn!("EAGAIN during first message read");
                    self.rearm(cx, r0z::POLLIN)?;
                    return Poll::Pending;
                }
                Err(e) => return Poll::Ready(Err(e.into())),
            }
        }

        assert!(!buffer.is_empty());
        Poll::Ready(Ok(buffer))
    }

    /// Attempt to send a multipart message.
    ///
    /// Sending the whole message at once may not be possible.
    /// If the function returns `Poll::Ready(Ok(()))`, the whole message has been sent.
    /// If the function returns `Poll::Pending`, the remaining message in the buffer should be
    /// attempted to be written the next time the socket is polled.
    pub(crate) fn multipart_send(&self, buffer: &mut Multipart) -> Poll<Result<()>> {
        let len = buffer.len();

        while let Some(mut msg) = buffer.pop_front() {
            let mut flags = r0z::DONTWAIT;
            if !buffer.is_empty() {
                flags |= r0z::SNDMORE;
            }

            let result = self.get_socket().send_message(&mut msg, flags);
            self.after_io(r0z::POLLOUT)?;
            match result {
                Ok(_) => {}
                Err(r0z::Error::EAGAIN) => {
                    buffer.push_front(msg);

                    // If this was not the first message, we return a special error.
                    if buffer.len() != len {
                        return Poll::Ready(Err(InterruptedSend));
                    }
                    return Poll::Pending;
                }
                Err(e) => return Poll::Ready(Err(e.into())),
            }
        }

        Poll::Ready(Ok(()))
    }

    /// Attempt to flush the message buffer.
    /// If the buffer cannot be fully flushed, `Poll::Pending` will be returned and a wakeup
    /// will be scheduled the next time there is an event on the ZMQ socket.
    pub(crate) fn multipart_flush(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut Multipart,
    ) -> Poll<Result<()>> {
        while !buffer.is_empty() {
            ready!(self.multipart_poll_write_ready(cx))?;
            match self.multipart_send(buffer) {
                Poll::Ready(result) => result?,
                Poll::Pending => {
                    self.rearm(cx, r0z::POLLOUT)?;
                    return Poll::Pending;
                }
            }
        }

        assert!(buffer.is_empty());
        Poll::Ready(Ok(()))
    }

    /// Returns `Poll::Ready(Ok(()))` if the given ZMQ socket is ready for writing.
    /// Returns `Poll::Pending` and schedules a wakeup on the next event for the socket otherwise.
    pub(crate) fn multipart_poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.multipart_poll(cx, r0z::POLLOUT)
    }

    /// Returns `Poll::Ready(Ok(()))` if the given ZMQ socket is ready for reading.
    /// Returns `Poll::Pending` and schedules a wakeup on the next event for the socket otherwise.
    pub(crate) fn multipart_poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.multipart_poll(cx, r0z::POLLIN)
    }

    fn multipart_poll(&self, cx: &mut Context<'_>, event: r0z::PollEvents) -> Poll<Result<()>> {
        let waiter = if event == r0z::POLLIN {
            &self.waiters.read
        } else {
            &self.waiters.write
        };
        waiter.register(cx.waker());
        let waker = waker_ref(&self.waiters);
        let mut combined = Context::from_waker(&waker);

        // Arm before querying ZMQ_EVENTS, which acknowledges native notification edges.
        // Consume a stale runtime notification and rearm before returning Pending.
        for _ in 0..2 {
            let notification = self.registration.poll_readable(&mut combined)?;
            let events = self.get_socket().get_events()?;
            self.wake_opposite(events, event);
            if events.contains(event) {
                waiter.take();
                return Poll::Ready(Ok(()));
            }
            if notification.is_pending() {
                return Poll::Pending;
            }
        }
        // Bound work per poll if notifications keep arriving during registration.
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn rearm(&self, cx: &mut Context<'_>, event: r0z::PollEvents) -> Result<()> {
        if let Poll::Ready(result) = self.multipart_poll(cx, event) {
            result?;
            cx.waker().wake_by_ref();
        }
        Ok(())
    }
}

/// Buffer used by receiver implementations to hold multiparts.
pub(crate) struct ReceiverBuffer {
    capacity: usize,
    buffer: VecDeque<Multipart>,
}

impl ReceiverBuffer {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        let buffer = VecDeque::with_capacity(capacity);
        Self { capacity, buffer }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    pub(crate) fn is_full(&self) -> bool {
        self.buffer.len() == self.capacity
    }

    #[inline]
    pub(crate) fn pop_front(&mut self) -> Option<Multipart> {
        self.buffer.pop_front()
    }

    #[inline]
    pub(crate) fn push_back(&mut self, item: Multipart) {
        self.buffer.push_back(item)
    }
}
