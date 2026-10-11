use comparison_common::{Backend, Config, Pattern, Result, Role};
use omq_tokio::{Context, ContextConfig, Message, Options, Socket, SocketType};

struct Omq {
    socket: Socket,
    _context: Context,
}

impl Backend for Omq {
    type Message = Message;

    async fn open(config: &Config) -> Result<(Self, String)> {
        let context = Context::with_config(ContextConfig {
            io_threads: config.io_threads,
        });
        let kind = match (config.pattern, config.role) {
            (Pattern::Pushpull, Role::Sender) => SocketType::Push,
            (Pattern::Pushpull, Role::Receiver) => SocketType::Pull,
            (Pattern::Reqrep, Role::Sender) => SocketType::Req,
            (Pattern::Reqrep, Role::Receiver) => SocketType::Rep,
        };
        let options = Options::default()
            .send_hwm(config.hwm as u32)
            .recv_hwm(config.hwm as u32)
            .linger(std::time::Duration::ZERO);
        let socket = context.socket(kind, options);
        let endpoint = if config.role == Role::Receiver {
            socket.bind("tcp://127.0.0.1:0".parse()?).await?.to_string()
        } else {
            socket.connect(config.endpoint.parse()?).await?;
            config.endpoint.clone()
        };
        Ok((
            Self {
                socket,
                _context: context,
            },
            endpoint,
        ))
    }

    fn message(payload: Vec<u8>) -> Message {
        Message::single(payload)
    }
    fn frame_count(message: &Message) -> usize {
        message.len()
    }
    fn payload(message: &Message) -> &[u8] {
        message.part_slice(0).unwrap()
    }
    fn native_version() -> Option<(i32, i32, i32)> {
        None
    }
    async fn send(&mut self, message: Message) -> Result<()> {
        self.socket.send(message).await?;
        Ok(())
    }
    async fn recv(&mut self) -> Result<Message> {
        Ok(self.socket.recv().await?)
    }
}

fn main() -> Result<()> {
    comparison_common::main::<Omq>()
}
