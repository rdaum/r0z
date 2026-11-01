# r0z-async

Async Rust bindings to native ZeroMQ (`libzmq`), built on `r0z`. This is a fork of `tmq` 0.5.0,
maintained in the r0z workspace. It is not a ZeroMQ implementation written from scratch in Rust.

The built-in runtime adapters support Tokio, `async-io`, and compio on Unix. Tokio and CURVE
encryption are enabled by default. Supported socket patterns include request/reply,
publish/subscribe, dealer/router, and push/pull.

```rust,no_run
use futures::SinkExt;
use r0z_async::{publish, Context, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new();
    let mut socket = publish(&context).bind("tcp://127.0.0.1:7899")?;
    socket.send(vec!["topic", "hello"]).await?;
    Ok(())
}
```

This example needs `futures` and Tokio with the `macros` and `rt-multi-thread` features. See
[examples](https://github.com/rdaum/r0z/tree/main/crates/r0z-async/examples) for further usage.
Publish sockets deliver messages only to connected subscribers; this example does not wait for them.

See the [development tickets](https://github.com/rdaum/r0z/blob/main/README.md#development) for
known concerns and planned improvements. Public names such as `TmqError` and `AsZmqSocket` remain
unchanged.

See the [workspace README](https://github.com/rdaum/r0z/blob/main/README.md) for setup, migration,
maintenance history, and current limitations. See
[UPSTREAM.md](https://github.com/rdaum/r0z/blob/main/crates/r0z-async/UPSTREAM.md) for the exact
source, authors, and license declarations. This crate is available under
[MIT](https://github.com/rdaum/r0z/blob/main/crates/r0z-async/LICENSE-MIT) or
[Apache-2.0](https://github.com/rdaum/r0z/blob/main/crates/r0z-async/LICENSE-APACHE), at your
choice.

## Runtime selection

For Tokio, keep the default features. Existing socket constructors still work inside an I/O-enabled
Tokio runtime. To select Tokio explicitly, call `.with_runtime::<r0z_async::runtime::Tokio>()`
before `bind` or `connect`.

For `async-io`, disable default features and select the adapter:

```toml
[dependencies]
r0z-async = { version = "0.3.0", default-features = false, features = ["async-io"] }
async-io = "2.6.0"
futures = "0.3.34"
```

```rust,no_run
use futures::{SinkExt, StreamExt};
use r0z_async::{pull, push, runtime::AsyncIo, Context, Result};

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

This configuration builds without Tokio or sodium. Add `"curve"` to the feature list if you need
CURVE encryption. For Tokio without sodium, use `default-features = false` with
`features = ["tokio"]`. Cargo combines dependency features, so another dependency can still enable
sodium.

For compio, disable default features and select its adapter:

```toml
[dependencies]
r0z-async = { version = "0.3.0", default-features = false, features = ["compio"] }
compio = { version = "0.19.2", features = ["macros"] }
futures = "0.3.34"
```

```rust,no_run
use futures::{SinkExt, StreamExt};
use r0z_async::{pull, push, runtime::Compio, Context, Result};

#[compio::main]
async fn main() -> Result<()> {
    let context = Context::new();
    let mut receiver = pull(&context)
        .with_runtime::<Compio>()
        .bind("inproc://compio")?;
    let mut sender = push(&context)
        .with_runtime::<Compio>()
        .connect("inproc://compio")?;
    sender.send(vec!["hello"]).await?;
    assert_eq!(receiver.next().await.unwrap()?[0].as_str(), Some("hello"));
    Ok(())
}
```

Add `"curve"` to retain CURVE encryption. The compio configuration needs no Tokio dependency or
runtime. See the
[`compio` example](https://github.com/rdaum/r0z/blob/main/crates/r0z-async/examples/compio.rs) for a
receive task.

Create and use compio sockets inside the same compio runtime. Compio sockets are neither `Send` nor
`Sync`. Use `compio::runtime::spawn` for local tasks. Split socket halves can run in separate tasks
on that runtime thread. Create a separate socket inside each runtime when you use multiple threads.
Builders do not hold a registration and can move between threads before `bind` or `connect`. Polling
outside the original compio runtime returns an I/O error, including on the same thread.

Compio's `FutureExt::with_cancel` works for send and receive futures. Cancellation returns an I/O
error with `ECANCELED`. Each direction observes its own token; cancelling one split half does not
cancel the other. Cancellation does not undo frames already delivered. Sink send buffers retain
queued frames; a later flush can send them. REQ/REP sockets treat cancellation-token errors like
other I/O errors: they enter `Failed` and must be recreated. Drop the borrowing REQ/REP future to
cancel a wait while preserving its state.

Polling a REQ/REP send or receive outside its original compio runtime also marks the socket
`Failed`, even when no native I/O occurred. Returning to the original runtime does not reset this
state. Recreate the socket before further I/O.

The adapter uses compio's `PollFd` on a duplicate of the native notification descriptor. On Linux,
compio can use io_uring or its polling driver. Other supported Unix targets use the polling driver.
Native libzmq still owns transport I/O and message buffers. The adapter never reads or writes the
notification descriptor. This integration does not establish a performance improvement.

The `async-io` adapter also works with executors such as smol. The default adapter follows this
feature priority: Tokio, async-io, then compio. Select an adapter explicitly when multiple features
are enabled. Different socket types and adapters can coexist in one application.

All adapters share multipart handling, native readiness checks, and separate send and receive
waiters. Each socket owns a concrete registration and one shared waker allocation. Runtime selection
uses static dispatch. Compio manages pending readiness operations through its driver.

All built-in adapters currently support Unix only. On Windows, `ZMQ_FD` returns a `SOCKET`. Windows
needs a separate validated registration path.

## Runtime API migration from 0.2

Socket types now include a runtime parameter. For example, a compio DEALER has type
`Dealer<Compio>`. An omitted parameter selects `runtime::DefaultRuntime` from the enabled features.
Existing calls that use the default adapter retain their syntax.

Cargo unifies features across dependencies. Another dependency that enables Tokio can therefore
change `DefaultRuntime` and break an inferred return type. Libraries should name the runtime in
their public socket types and select it with `with_runtime`, as shown below.

`with_runtime::<R>()` now changes the builder's output type. Update explicit return types and stored
socket types for adapters that differ from the default:

```rust,no_run
use r0z_async::{request_reply::RequestReply, runtime::AsyncIo, Context, Result};

fn connect(context: &Context) -> Result<RequestReply<AsyncIo>> {
    r0z_async::request(context)
        .with_runtime::<AsyncIo>()
        .connect("tcp://127.0.0.1:7897")
}
```

This example requires the `async-io` feature. Tokio and async-io socket types retain `Send`,
including when compio is enabled. Compio socket types remain local. Cargo feature unification does
not remove `Send` from an explicitly typed Tokio or async-io socket.

Custom adapters must define `Runtime::Registration` and return that concrete type from `register`.
`Registration` now requires `Unpin` instead of `Send`. Its ownership and notification requirements
remain unchanged. The socket derives its thread restrictions from the registration. The old boxed
registration and `runtime::Register` function pointer are removed. The optional `check_context` hook
receives the original task context before each I/O poll. Use it for runtime affinity or cancellation
checks; readiness polling receives the shared notification waker.

SUB sockets now register at `bind` or `connect`, before the first `subscribe` call. Registration
errors therefore return from the builder. Tokio construction outside a runtime returns an I/O error.
Enter an I/O-enabled Tokio runtime before `bind` or `connect`; a runtime without I/O still causes
Tokio registration to panic. Custom users of the hidden `FromZmqSocket` construction trait must also
update to its associated `WithRuntime<R>` type and socket-only constructor.

With no runtime feature, the default is `NoRuntime`. Constructors return an unsupported-operation
error unless the builder selects a custom adapter.

## Request/reply ownership and migration

`request()` and `reply()` now return `RequestReply`. This replaces `RequestSender` and
`RequestReceiver`. Both `send` and `recv` borrow the socket. Update stored socket types and remove
the assignments that transferred ownership between operations:

```rust,no_run
# async fn exchange(context: &r0z_async::Context) -> r0z_async::Result<()> {
let mut socket = r0z_async::request(context).connect("tcp://127.0.0.1:7897")?;
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
[`request_timeout` example](https://github.com/rdaum/r0z/blob/main/crates/r0z-async/examples/request_timeout.rs)
retains the socket after a timer wins `tokio::select!`, then receives the delayed reply.

## Shutdown and linger

Async sockets now default to `ZMQ_LINGER = 0`. Closing a socket discards its queued outgoing
messages. This prevents context shutdown from waiting indefinitely for those messages to reach a
peer. The synchronous `r0z` bindings retain the native default of infinite linger.

This changes the shutdown behaviour inherited from `tmq`. A successful `send()` or `flush()` means
that ZeroMQ accepted the message, not that the peer received it. Applications that need delivery
confirmation must use an application-level acknowledgement.

To wait for queued messages during context shutdown, explicitly set linger on the builder:

```rust,no_run
# fn connect(context: &r0z_async::Context) -> r0z_async::Result<()> {
let socket = r0z_async::request(context)
    .set_linger(1000) // Allow up to one second for queued messages.
    .connect("tcp://127.0.0.1:7897")?;
# Ok(())
# }
```

Use `-1` for infinite linger. Positive or infinite linger can block the thread that drops the final
context reference. If the socket holds that reference, dropping the socket also shuts down the
context. A receive timeout does not change an explicit linger setting.

If you selected nonzero linger and need to abandon an exchange without waiting, set linger to zero
before dropping the socket:

```rust,no_run
# fn abandon(socket: r0z_async::request_reply::RequestReply) -> r0z_async::Result<()> {
use r0z_async::SocketExt;
socket.set_linger(0)?;
drop(socket);
# Ok(())
# }
```

Compio cancellation can retain the duplicated notification descriptor until the driver processes it.
It does not retain the native socket or context. Dropping a socket closes it without another runtime
turn, subject to the native linger rules above.

This controls local shutdown. It does not undo requests already delivered to a peer. Configure
linger on each socket with queued outgoing messages before dropping the final context reference. See
the [native shutdown contract](https://libzmq.readthedocs.io/en/latest/zmq_ctx_term.html). The
[ownership ticket](https://github.com/rdaum/r0z/issues/1) and
[timeout investigation](https://github.com/rdaum/r0z/issues/2) track the related work.
