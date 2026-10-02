# r0rz

**r0rz** (pronounced “roars”) provides Rust bindings to native [ZeroMQ](https://zeromq.org/),
through the `libzmq` C API.

It is not a new implementation of ZeroMQ written in Rust.

Native `libzmq` handles messaging, transports, queues, and protocol behavior.

This workspace maintains synchronous bindings, native FFI bindings, and async bindings together.

It continues the work of [`rust-zmq`](https://github.com/erickt/rust-zmq) and
[`tmq`](https://github.com/cetra3/tmq).

[Changes](NEWS.md) · [Build and test](HACKING.md) · [Contributing](CONTRIBUTING.md) ·
[Source and licenses](PROVENANCE.md)

## Why this fork exists

I started this fork because my downstream projects, including [mooR](https://github.com/rdaum/moor),
needed maintained ZeroMQ bindings -- both sync and async. They used both `zmq` (which was abandoned)
and `tmq` (which depended on it), so problems in either crate affected them. Also, while other
people have created from-scratch native Rust bindings but a) I don't yet trust their provenance /
status b) I need both sync and async implementations and not to be tied to tokio everywhere.

By March 2026, `rust-zmq` was effectively abandoned. Its last published `zmq` release was
[0.10.0](https://crates.io/crates/zmq/0.10.0), from November 2022. Its latest upstream commit was
from [May 2025](https://github.com/erickt/rust-zmq/commit/5d78967001abb1aece2fba878d6151cb66cd1767).
Fixes and release requests remained open.

The upstream owner
[said they no longer used the project](https://github.com/erickt/rust-zmq/issues/402#issuecomment-3981188261)
and were open to a handoff. I
[offered to take over maintenance](https://github.com/erickt/rust-zmq/issues/402#issuecomment-3981192243).
As of October 2, 2026, that offer had received no reply, and no handoff had occurred. So this is now
an independent fork.

`tmq` 0.5.0 depended on `zmq` 0.10.0 and so inherited its maintenance problem. Its async API also
needs attention. The [open tickets](#development) track those concerns and link to the original
upstream reports.

Keeping the crates together lets changes to the native, synchronous, and async layers be tested
together. The new name(s) are meant give this maintenance work its own release path.

## Workspace

| Package                           | Rust import  | Purpose                                      | Current platform support     |
| --------------------------------- | ------------ | -------------------------------------------- | ---------------------------- |
| [`r0rz`](crates/r0rz)             | `r0rz`       | Safe synchronous bindings to `libzmq`        | Linux, macOS, Windows        |
| [`r0rz-sys`](crates/r0rz-sys)     | `r0rz_sys`   | Native library build and unsafe FFI bindings | Linux, macOS, Windows        |
| [`r0rz-async`](crates/r0rz-async) | `r0rz_async` | Async sockets built on `r0rz`                | Unix, with Tokio or async-io |

The synchronous API follows the native C API closely. The async crate supports request/reply,
publish/subscribe, dealer/router, and push/pull sockets. It provides `futures` streams and sinks
where the socket pattern permits them.

The async crate supports Tokio and `async-io` on Unix. See
[adapter selection](crates/r0rz-async/README.md#runtime-selection) for feature flags and examples.

## Development

The fork integrates upstream fixes and maintains all three crates in one workspace. See
[NEWS.md](NEWS.md) for completed changes and original pull request numbers.

Open tickets cover
[socket ownership and cancellation (#1)](https://github.com/rdaum/rust-zmq/issues/1),
[receive timeouts (#2)](https://github.com/rdaum/rust-zmq/issues/2), and
[split socket hangs (#3)](https://github.com/rdaum/rust-zmq/issues/3). The
[runtime adapter work (#4)](https://github.com/rdaum/rust-zmq/issues/4) records the portability
requirements. [Message copies (#5)](https://github.com/rdaum/rust-zmq/issues/5) tracks further
performance work. The tickets contain the evidence, proposed work, and acceptance criteria.

## Use the bindings

The renamed crates are not yet published. For development, use local paths to this checkout:

```toml
[dependencies]
r0rz = { path = "../rust-zmq/crates/r0rz" }
# Add this dependency if you need async sockets:
r0rz-async = { path = "../rust-zmq/crates/r0rz-async" }
```

Adjust these paths for your project. Applications normally need no direct dependency on `r0rz-sys`.

This example sends a message between two sockets in one process:

```rust
fn main() -> r0rz::Result<()> {
    let context = r0rz::Context::new();
    let sender = context.socket(r0rz::PAIR)?;
    let receiver = context.socket(r0rz::PAIR)?;
    sender.set_linger(0)?;
    receiver.set_linger(0)?;

    receiver.bind("inproc://example")?;
    sender.connect("inproc://example")?;
    sender.send("hello", 0)?;
    assert_eq!(receiver.recv_bytes(0)?, b"hello");
    Ok(())
}
```

See the [synchronous examples](crates/r0rz/examples) and
[async examples](crates/r0rz-async/examples) for more socket patterns. To generate API
documentation, run:

```sh
cargo doc --workspace --no-deps --open
```

On Windows, add `--exclude r0rz-async`.

## Migrate from `zmq` and `tmq`

Replace the Cargo dependencies and Rust imports:

| Previous package | New package  | Import change              |
| ---------------- | ------------ | -------------------------- |
| `zmq`            | `r0rz`       | `zmq::` → `r0rz::`         |
| `zmq-sys`        | `r0rz-sys`   | `zmq_sys::` → `r0rz_sys::` |
| `tmq`            | `r0rz-async` | `tmq::` → `r0rz_async::`   |

The existing async type names, including `TmqError` and `AsZmqSocket`, remain unchanged. To reduce
source edits, you can use Cargo dependency aliases:

```toml
[dependencies]
zmq = { package = "r0rz", path = "../rust-zmq/crates/r0rz" }
tmq = { package = "r0rz-async", path = "../rust-zmq/crates/r0rz-async" }
```

Remove old `[patch.crates-io]` entries for this fork's `zmq` and `zmq-sys` packages. The new
packages cannot replace those names through a patch alone. Migrate dependencies that still use the
old bindings too. Both native binding packages declare `links = "zmq"`, so Cargo cannot include both
in one dependency graph.

## Build and test

Use current stable Rust and a native C/C++ build toolchain. The build compiles `libzmq` from source
through `zeromq-src`; the current dependency baseline bundles ZeroMQ 4.3.5. The synchronous crate
enables libsodium support for CURVE encryption. Installing a system ZeroMQ library alone does not
replace this source build.

On Linux or macOS:

```sh
cargo test --workspace --all-targets
cargo test --workspace --doc
cargo clippy --workspace --all-targets -- -D warnings
```

On Windows, add `--exclude r0rz-async` to each command. See [HACKING.md](HACKING.md) for more
commands, including the separate fuzz and consumer projects.

## Native behavior to account for

Configure process environment variables before creating contexts or starting threads that use
`libzmq`. Native code can read the environment while it runs. Concurrent environment changes can be
unsafe.

Sockets use infinite linger by default (`ZMQ_LINGER = -1`). Context shutdown can wait indefinitely
for pending outbound messages. For shutdown that discards queued messages, set
`socket.set_linger(0)`.

## Source and licenses

The synchronous and FFI crates descend from `erickt/rust-zmq` at commit
[`5d78967001abb1aece2fba878d6151cb66cd1767`](https://github.com/erickt/rust-zmq/commit/5d78967001abb1aece2fba878d6151cb66cd1767).
The async crate was imported from `cetra3/tmq` 0.5.0 at commit
[`538cce9b0fed9dd90a4bb3bcf28eb54e1f94973b`](https://github.com/cetra3/tmq/commit/538cce9b0fed9dd90a4bb3bcf28eb54e1f94973b).
Original authors and the existing Rust binding license terms are retained.

Both upstream Rust projects declare **MIT OR Apache-2.0**, which permits this combined workspace
under the same terms. You may choose either license for the Rust bindings. See
[LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE), and the
[async license notices](crates/r0rz-async/UPSTREAM.md).

Native dependencies retain their own licenses: `libzmq` uses MPL-2.0, and libsodium uses ISC. These
can be combined with the Rust bindings, subject to their notice and source distribution
requirements. See [PROVENANCE.md](PROVENANCE.md) for source links, attribution, and the license
review.

Unless you explicitly state otherwise, contributions are licensed under both MIT and Apache-2.0,
without additional terms.
