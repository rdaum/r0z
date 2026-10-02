# r0rz-async

Async Rust bindings to native ZeroMQ (`libzmq`), built on `r0rz`. This is a fork of `tmq` 0.5.0,
maintained in the r0rz workspace. It is not a ZeroMQ implementation written from scratch in Rust.

The built-in runtime adapters support Tokio and `async-io` on Unix. Tokio is enabled by default.
Supported socket patterns include request/reply, publish/subscribe, dealer/router, and push/pull.

```rust,no_run
use futures::SinkExt;
use r0rz_async::{publish, Context, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new();
    let mut socket = publish(&context).bind("tcp://127.0.0.1:7899")?;
    socket.send(vec!["topic", "hello"]).await?;
    Ok(())
}
```

This example needs `futures` and Tokio with the `macros` and `rt-multi-thread` features. See
[examples](examples) for further usage. Publish sockets deliver messages only to connected
subscribers; this example does not wait for them.

See the [development tickets](../../README.md#development) for known concerns and planned
improvements. Public names such as `TmqError` and `AsZmqSocket` remain unchanged.

See the [workspace README](../../README.md) for setup, migration, maintenance history, and current
limitations. See [UPSTREAM.md](UPSTREAM.md) for the exact source, authors, and license declarations.
This crate is available under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your choice.

## Runtime selection

For Tokio, keep the default features. Existing socket constructors still work inside an I/O-enabled
Tokio runtime. To select Tokio explicitly, call `.with_runtime::<r0rz_async::runtime::Tokio>()`
before `bind` or `connect`.

For `async-io`, disable default features:

```toml
[dependencies]
r0rz-async = { path = "../rust-zmq/crates/r0rz-async", default-features = false, features = ["async-io"] }
async-io = "2.6.0"
futures = "0.3.34"
```

```rust,no_run
use futures::{SinkExt, StreamExt};
use r0rz_async::{pull, push, runtime::AsyncIo, Context, Result};

fn main() -> Result<()> {
    async_io::block_on(async {
        let context = Context::new();
        let mut receiver = pull(&context)
            .with_runtime::<AsyncIo>()
            .set_linger(0)
            .bind("inproc://example")?;
        let mut sender = push(&context)
            .with_runtime::<AsyncIo>()
            .set_linger(0)
            .connect("inproc://example")?;
        sender.send(vec!["hello"]).await?;
        assert_eq!(receiver.next().await.unwrap()?[0].as_str(), Some("hello"));
        Ok(())
    })
}
```

This configuration builds without Tokio. The `async-io` adapter also works with executors such as
smol. When both features are enabled, Tokio remains the default. Select `AsyncIo` explicitly to use
it. Different sockets can use different adapters in the same application.

The shared core owns multipart handling, native readiness checks, and separate send and receive
waiters. Each adapter owns a native socket and its reactor registration. Each socket uses one boxed
registration and one shared waker allocation. The adapter boundary adds no allocation per message.
No performance improvement is claimed for this change.

For another runtime, implement `runtime::Runtime` and `runtime::Registration` and select it through
the builder. With neither built-in feature, constructors require an explicit adapter; otherwise
registration returns an unsupported-operation error.

Both built-in adapters currently support Unix only. On Windows, `ZMQ_FD` returns a `SOCKET`, not a
Unix file descriptor. Windows needs a separate validated registration path. This change does not
establish Windows or compio support.

## Request/reply ownership and migration

`request()` and `reply()` now return `RequestReply`. This replaces `RequestSender` and
`RequestReceiver`. Both `send` and `recv` borrow the socket. Update stored socket types and remove
the assignments that transferred ownership between operations:

```rust,no_run
# async fn exchange(context: &r0rz_async::Context) -> r0rz_async::Result<()> {
let mut socket = r0rz_async::request(context).connect("tcp://127.0.0.1:7897")?;
socket.send(vec!["hello"].into()).await?;
let reply = socket.recv().await?;
# Ok(())
# }
```

Use `state()` to inspect the next permitted operation:

| State          | Permitted operation                                         |
| -------------- | ----------------------------------------------------------- |
| `SendReady`    | Send a new multipart.                                       |
| `SendPending`  | Call `flush()` to finish the buffered multipart.            |
| `ReceiveReady` | Receive the next multipart.                                 |
| `Failed`       | Inspect or drop the socket. Recreate it before further I/O. |

A REQ socket starts in `SendReady`. A REP socket starts in `ReceiveReady`. Successful operations
alternate between sending and receiving. Invalid calls return `InvalidRequestReplyState` without
changing the socket state. An empty multipart returns `EINVAL`. One empty frame is a valid message.

Cancelling a pending receive preserves the socket and its protocol state. Call `recv()` again to
wait for the same reply. A timeout does not permit a REQ socket to send another request. To abandon
that exchange, drop the socket and create another one.

Once polled, a pending send keeps its multipart in the socket. Call `flush()` to resume it. Another
`send()` cannot replace that message. If a send future is never polled, dropping it leaves the
socket unchanged and drops its message. Successful sends remain sent after later cancellation.

Native and reactor errors leave the socket owned by the caller but mark it `Failed`. An error can
occur after some multipart frames were sent or received. This API requires socket replacement
because it cannot establish safe recovery from every such error. Validation errors do not mark the
socket as failed.

Use native socket access for inspection and configuration only. Direct native I/O and relaxed REQ
sequencing bypass the protocol state tracked by this wrapper.

Async operations use native `DONTWAIT`. Native `RCVTIMEO` and `SNDTIMEO` do not set async deadlines.
Use the timer or selection API of your runtime. The
[`request_timeout` example](examples/request_timeout.rs) retains the socket after a timer wins
`tokio::select!`, then receives the delayed reply.

Cancellation does not change native linger. The default linger can delay context shutdown while
outgoing messages remain undelivered. Set linger to zero before shutdown to discard queued messages
when this is acceptable. The [ownership ticket](https://github.com/rdaum/rust-zmq/issues/1) and
[timeout investigation](https://github.com/rdaum/rust-zmq/issues/2) record the related work.
