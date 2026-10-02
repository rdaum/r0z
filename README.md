# r0z

**r0z** provides Rust bindings to native [ZeroMQ](https://zeromq.org/), through the `libzmq` C API.

[![Crates.io](https://img.shields.io/crates/v/r0z.svg)](https://crates.io/crates/r0z)
[![Documentation](https://docs.rs/r0z/badge.svg)](https://docs.rs/r0z)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache%202.0-blue.svg)](https://github.com/rdaum/r0z/blob/main/PROVENANCE.md)
[![Sponsor](https://img.shields.io/badge/Sponsor-%E2%9D%A4-pink)](https://github.com/sponsors/rdaum)

It is not a new implementation of ZeroMQ written in Rust.

Native `libzmq` handles messaging, transports, queues, and protocol behavior.

This workspace maintains synchronous bindings, native FFI bindings, and async bindings together.

It continues the work of [`rust-zmq`](https://github.com/erickt/rust-zmq) and
[`tmq`](https://github.com/cetra3/tmq).

[Changes](https://github.com/rdaum/r0z/blob/main/NEWS.md) ·
[Build and test](https://github.com/rdaum/r0z/blob/main/HACKING.md) ·
[Contributing](https://github.com/rdaum/r0z/blob/main/CONTRIBUTING.md) ·
[Source and licenses](https://github.com/rdaum/r0z/blob/main/PROVENANCE.md)

## Why this fork exists

I started this fork because my downstream projects, including [mooR](https://github.com/rdaum/moor),
needed maintained ZeroMQ bindings -- both sync and async. They used both `zmq` (which was abandoned)
and `tmq` (which depended on it), so problems in either crate affected them. Also, while other
people have created from-scratch native Rust bindings: a) I don't yet trust their provenance /
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

`tmq` 0.5.0 depended on `zmq` 0.10.0 and so inherited its maintenance problem. The
[development tickets](#development) record the async concerns, upstream reports, and work completed
in this fork.

Keeping the crates together lets changes to the native, synchronous, and async layers be tested
together. The new package names give this maintenance work its own release path.

## Workspace

| Package                                                                | Rust import | Purpose                                      | Current platform support     |
| ---------------------------------------------------------------------- | ----------- | -------------------------------------------- | ---------------------------- |
| [`r0z`](https://github.com/rdaum/r0z/tree/main/crates/r0z)             | `r0z`       | Safe synchronous bindings to `libzmq`        | Linux, macOS, Windows        |
| [`r0z-sys`](https://github.com/rdaum/r0z/tree/main/crates/r0z-sys)     | `r0z_sys`   | Native library build and unsafe FFI bindings | Linux, macOS, Windows        |
| [`r0z-async`](https://github.com/rdaum/r0z/tree/main/crates/r0z-async) | `r0z_async` | Async sockets built on `r0z`                 | Unix, with Tokio or async-io |

The synchronous API follows the native C API closely. The async crate supports request/reply,
publish/subscribe, dealer/router, and push/pull sockets. It provides `futures` streams and sinks
where the socket pattern permits them.

The async crate supports Tokio and `async-io` on Unix. See
[adapter selection](https://github.com/rdaum/r0z/blob/main/crates/r0z-async/README.md#runtime-selection)
for feature flags and examples.

## Development

The fork integrates upstream fixes and maintains all three crates in one workspace. See
[NEWS.md](https://github.com/rdaum/r0z/blob/main/NEWS.md) for completed changes and original pull
request numbers.

The tickets record the completed work on
[socket ownership and cancellation (#1)](https://github.com/rdaum/r0z/issues/1),
[receive timeouts and shutdown (#2)](https://github.com/rdaum/r0z/issues/2),
[split socket regressions (#3)](https://github.com/rdaum/r0z/issues/3), and
[message copies (#5)](https://github.com/rdaum/r0z/issues/5). The
[runtime adapter ticket (#4)](https://github.com/rdaum/r0z/issues/4) records adapter support and CI
follow-up. See the [open issues](https://github.com/rdaum/r0z/issues?q=is%3Aissue%20is%3Aopen) for
remaining work.

## Use the bindings

The first release uses version `0.1.0` for all three packages. Its dependency declarations are:

```toml
[dependencies]
r0z = "0.1.0"
# Add this dependency if you need async sockets:
r0z-async = "0.1.0"
```

Applications normally need no direct dependency on `r0z-sys`. For local development, replace the
version with a path to the package under `crates/`.

This example sends a message between two sockets in one process:

```rust
fn main() -> r0z::Result<()> {
    let context = r0z::Context::new();
    let sender = context.socket(r0z::PAIR)?;
    let receiver = context.socket(r0z::PAIR)?;
    sender.set_linger(0)?;
    receiver.set_linger(0)?;

    receiver.bind("inproc://example")?;
    sender.connect("inproc://example")?;
    sender.send("hello", 0)?;
    assert_eq!(receiver.recv_bytes(0)?, b"hello");
    Ok(())
}
```

See the [synchronous examples](https://github.com/rdaum/r0z/tree/main/crates/r0z/examples) and
[async examples](https://github.com/rdaum/r0z/tree/main/crates/r0z-async/examples) for more socket
patterns. To generate API documentation, run:

```sh
cargo doc --workspace --no-deps --open
```

On Windows, add `--exclude r0z-async`.

## Migrate from `zmq` and `tmq`

Replace the Cargo dependencies and Rust imports:

| Previous package | New package | Import change             |
| ---------------- | ----------- | ------------------------- |
| `zmq`            | `r0z`       | `zmq::` → `r0z::`         |
| `zmq-sys`        | `r0z-sys`   | `zmq_sys::` → `r0z_sys::` |
| `tmq`            | `r0z-async` | `tmq::` → `r0z_async::`   |

`TmqError` and `AsZmqSocket` retain their names. Request/reply sockets now use `RequestReply` with
borrowing operations. See the
[async migration guide](https://github.com/rdaum/r0z/blob/main/crates/r0z-async/README.md#requestreply-ownership-and-migration).
To reduce import edits, you can use Cargo dependency aliases:

```toml
[dependencies]
zmq = { package = "r0z", version = "0.1.0" }
tmq = { package = "r0z-async", version = "0.1.0" }
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

On Windows, add `--exclude r0z-async` to each command. See
[HACKING.md](https://github.com/rdaum/r0z/blob/main/HACKING.md) for more commands, including the
separate fuzz and consumer projects.

## Native behavior to account for

Configure process environment variables before creating contexts or starting threads that use
`libzmq`. Native code can read the environment while it runs. Concurrent environment changes can be
unsafe.

Synchronous sockets use infinite linger by default (`ZMQ_LINGER = -1`). Their context shutdown can
wait indefinitely for pending outbound messages. Set `socket.set_linger(0)` to discard queued
messages on close.

Async sockets default to zero linger. Closing them discards queued outgoing messages. Set linger
explicitly to permit a finite or infinite wait during context shutdown. See the
[async shutdown guide](https://github.com/rdaum/r0z/blob/main/crates/r0z-async/README.md#shutdown-and-linger).

## Support development

> If `r0z` is useful in your work, consider sponsoring development on
> [GitHub Sponsors](https://github.com/sponsors/rdaum). I also offer consulting in systems
> engineering, profiling and performance tuning, and Rust development. My experience includes 10
> years at Google and more than 25 years in software development. If your team needs help, feel free
> to reach out.

## Source and licenses

The synchronous and FFI crates descend from `erickt/rust-zmq` at commit
[`5d78967001abb1aece2fba878d6151cb66cd1767`](https://github.com/erickt/rust-zmq/commit/5d78967001abb1aece2fba878d6151cb66cd1767).
The async crate was imported from `cetra3/tmq` 0.5.0 at commit
[`538cce9b0fed9dd90a4bb3bcf28eb54e1f94973b`](https://github.com/cetra3/tmq/commit/538cce9b0fed9dd90a4bb3bcf28eb54e1f94973b).
Original authors and the existing Rust binding license terms are retained.

Both upstream Rust projects declare **MIT OR Apache-2.0**, which permits this combined workspace
under the same terms. You may choose either license for the Rust bindings. See
[LICENSE-MIT](https://github.com/rdaum/r0z/blob/main/LICENSE-MIT),
[LICENSE-APACHE](https://github.com/rdaum/r0z/blob/main/LICENSE-APACHE), and the
[async license notices](https://github.com/rdaum/r0z/blob/main/crates/r0z-async/UPSTREAM.md).

Native dependencies retain their own licenses: `libzmq` uses MPL-2.0, and libsodium uses ISC. These
can be combined with the Rust bindings, subject to their notice and source distribution
requirements. See [PROVENANCE.md](https://github.com/rdaum/r0z/blob/main/PROVENANCE.md) for source
links, attribution, and the license review.

Unless you explicitly state otherwise, contributions are licensed under both MIT and Apache-2.0,
without additional terms.
