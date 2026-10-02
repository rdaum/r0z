# r0rz-async

Async Rust bindings to native ZeroMQ (`libzmq`), built on `r0rz`.
This is a fork of `tmq` 0.5.0, maintained in the r0rz workspace.
It is not a ZeroMQ implementation written from scratch in Rust.

The current implementation requires Tokio and Unix.
[Runtime portability](https://github.com/rdaum/rust-zmq/issues/4) is planned but not implemented.
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

This example needs `futures` and Tokio with the `macros` and `rt-multi-thread` features.
See [examples](examples) for further usage.
Publish sockets deliver messages only to connected subscribers; this example does not wait for them.

See the [development tickets](../../README.md#development) for known concerns and planned improvements.
Public names such as `TmqError` and `AsZmqSocket` remain unchanged.

See the [workspace README](../../README.md) for setup, migration, maintenance history, and current limitations.
See [UPSTREAM.md](UPSTREAM.md) for the exact source, authors, and license declarations.
This crate is available under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your choice.
