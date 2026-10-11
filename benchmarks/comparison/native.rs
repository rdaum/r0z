use api::AsZmqSocket;
use comparison_common::futures::{SinkExt, StreamExt};
use comparison_common::{Backend, Config, Pattern, Result, Role};

enum Socket {
    Push(api::push::Push),
    Pull(api::pull::Pull),
    #[cfg(not(feature = "tmq"))]
    RequestReply(api::request_reply::RequestReply),
    #[cfg(feature = "tmq")]
    Send(Option<api::request_reply::RequestSender>),
    #[cfg(feature = "tmq")]
    Receive(Option<api::request_reply::RequestReceiver>),
}

struct Native {
    socket: Socket,
    // Keep the context alive until after the socket is dropped.
    _context: native::Context,
}

impl Backend for Native {
    type Message = api::Multipart;

    async fn open(config: &Config) -> Result<(Self, String)> {
        let context = native::Context::new();
        context.set_io_threads(1)?;
        macro_rules! open {
            ($builder:expr, $variant:ident) => {{
                let builder = $builder
                    .set_linger(0)
                    .set_sndhwm(config.hwm)
                    .set_rcvhwm(config.hwm);
                let socket = if config.role == Role::Receiver {
                    builder.bind("tcp://127.0.0.1:*")?
                } else {
                    builder.connect(&config.endpoint)?
                };
                let endpoint = socket
                    .get_socket()
                    .get_last_endpoint()?
                    .map_err(|_| "non-UTF8 endpoint")?;
                (Socket::$variant(socket), endpoint)
            }};
        }
        let (socket, endpoint) = match (config.pattern, config.role) {
            (Pattern::Pushpull, Role::Sender) => open!(api::push(&context), Push),
            (Pattern::Pushpull, Role::Receiver) => open!(api::pull(&context), Pull),
            #[cfg(not(feature = "tmq"))]
            (Pattern::Reqrep, role) => {
                let builder = if role == Role::Sender {
                    api::request(&context)
                } else {
                    api::reply(&context)
                };
                open!(builder, RequestReply)
            }
            #[cfg(feature = "tmq")]
            (Pattern::Reqrep, Role::Sender) => {
                let socket = api::request(&context)
                    .set_linger(0)
                    .set_sndhwm(config.hwm)
                    .set_rcvhwm(config.hwm)
                    .connect(&config.endpoint)?;
                (Socket::Send(Some(socket)), config.endpoint.clone())
            }
            #[cfg(feature = "tmq")]
            (Pattern::Reqrep, Role::Receiver) => {
                let socket = api::reply(&context)
                    .set_linger(0)
                    .set_sndhwm(config.hwm)
                    .set_rcvhwm(config.hwm)
                    .bind("tcp://127.0.0.1:*")?;
                let endpoint = socket
                    .get_socket()
                    .get_last_endpoint()?
                    .map_err(|_| "non-UTF8 endpoint")?;
                (Socket::Receive(Some(socket)), endpoint)
            }
        };
        Ok((
            Self {
                socket,
                _context: context,
            },
            endpoint,
        ))
    }

    fn message(payload: Vec<u8>) -> Self::Message {
        api::Multipart::from(native::Message::from(payload))
    }

    fn frame_count(message: &Self::Message) -> usize {
        message.len()
    }
    fn payload(message: &Self::Message) -> &[u8] {
        &message[0]
    }
    fn native_version() -> Option<(i32, i32, i32)> {
        Some(native::version())
    }

    async fn send(&mut self, message: Self::Message) -> Result<()> {
        match &mut self.socket {
            Socket::Push(socket) => socket.send(message).await?,
            #[cfg(not(feature = "tmq"))]
            Socket::RequestReply(socket) => socket.send(message).await?,
            #[cfg(feature = "tmq")]
            Socket::Send(socket) => {
                self.socket = Socket::Receive(Some(socket.take().unwrap().send(message).await?));
            }
            _ => return Err("send on receiving socket".into()),
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Self::Message> {
        match &mut self.socket {
            Socket::Pull(socket) => Ok(socket.next().await.ok_or("stream ended")??),
            #[cfg(not(feature = "tmq"))]
            Socket::RequestReply(socket) => Ok(socket.recv().await?),
            #[cfg(feature = "tmq")]
            Socket::Receive(socket) => {
                let (message, sender) = socket.take().unwrap().recv().await?;
                self.socket = Socket::Send(Some(sender));
                Ok(message)
            }
            _ => Err("receive on sending socket".into()),
        }
    }
}

fn main() -> Result<()> {
    comparison_common::main::<Native>()
}
