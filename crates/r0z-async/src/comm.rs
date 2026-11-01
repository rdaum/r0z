use crate::runtime::Runtime;
use crate::{
    Multipart,
    poll::{ReceiverBuffer, ZmqPoller},
};

/// Sends multiparts using an owned Poller.
pub struct Sender<R: Runtime> {
    pub(crate) poller: ZmqPoller<R>,
    pub(crate) buffer: Multipart,
}
impl_as_socket!(Sender, poller);
impl_sink!(Sender, buffer, poller);

impl<R: Runtime> Sender<R> {
    pub(crate) fn new(poller: ZmqPoller<R>) -> Self {
        Self {
            poller,
            buffer: Default::default(),
        }
    }
}

/// Receives multiparts using an owned Poller.
pub struct Receiver<R: Runtime> {
    pub(crate) poller: ZmqPoller<R>,
}
impl_as_socket!(Receiver, poller);
impl_stream!(Receiver, poller);

impl<R: Runtime> Receiver<R> {
    pub(crate) fn new(poller: ZmqPoller<R>) -> Self {
        Self { poller }
    }
    pub fn buffered(self, capacity: usize) -> BufferedReceiver<R> {
        BufferedReceiver::new(self.poller, capacity)
    }
}

/// Receives buffered multiparts using an owned Poller.
pub struct BufferedReceiver<R: Runtime> {
    pub(crate) poller: ZmqPoller<R>,
    pub(crate) buffer: ReceiverBuffer,
}
impl_as_socket!(BufferedReceiver, poller);
impl_buffered_stream!(BufferedReceiver, buffer, poller);

impl<R: Runtime> BufferedReceiver<R> {
    pub(crate) fn new(poller: ZmqPoller<R>, capacity: usize) -> Self {
        Self {
            poller,
            buffer: ReceiverBuffer::new(capacity),
        }
    }
}

/// Sends ands receives multiparts using an owned Poller.
pub struct SenderReceiver<R: Runtime> {
    pub(crate) poller: ZmqPoller<R>,
    pub(crate) buffer: Multipart,
}
impl_as_socket!(SenderReceiver, poller);
impl_sink!(SenderReceiver, buffer, poller);
impl_stream!(SenderReceiver, poller);

impl<R: Runtime> SenderReceiver<R> {
    pub(crate) fn new(poller: ZmqPoller<R>) -> Self {
        Self {
            poller,
            buffer: Default::default(),
        }
    }
}
