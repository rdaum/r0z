Rust ZeroMQ bindings.

[![Travis Build Status](https://travis-ci.org/erickt/rust-zmq.png?branch=master)](https://travis-ci.org/erickt/rust-zmq)
[![Appveyor Build status](https://ci.appveyor.com/api/projects/status/xhytsx4jwyb9qk7m?svg=true)](https://ci.appveyor.com/project/erickt/rust-zmq)
[![Coverage Status](https://coveralls.io/repos/erickt/erickt-zmq/badge.svg?branch=master)](https://coveralls.io/r/erickt/erickt-zmq?branch=master)
[![Apache 2.0 licensed](https://img.shields.io/badge/license-Apache2.0-blue.svg)](./LICENSE-APACHE)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE-MIT)
[![crates.io](http://meritbadge.herokuapp.com/zmq)](https://crates.io/crates/zmq)
[![docs](https://docs.rs/zmq/badge.svg)](https://docs.rs/zmq)

[Documentation](https://docs.rs/crate/zmq/)

[Release Notes](./NEWS.md)

# About

The `zmq` crate provides bindings for the `libzmq` library from the
[ZeroMQ](https://zeromq.org/) project. The API exposed by `zmq` should
be safe (in the usual Rust sense), but it follows the C API closely,
so it is not very idiomatic.

# Workspace

This repository contains a Cargo workspace for synchronous and asynchronous ZeroMQ bindings.

| Directory | Package | Purpose |
| --- | --- | --- |
| `crates/zmq` | `zmq` | Safe synchronous bindings |
| `crates/zmq-sys` | `zmq-sys` | Native build and FFI bindings |
| `crates/zmq-async` | `tmq` | Async bindings imported from `tmq` |

The async crate currently requires Tokio and Unix. Runtime independence is planned but not yet implemented.
Package names remain unchanged. The imported `tmq` package has publication disabled until it receives a new name.
See [async import details](crates/zmq-async/UPSTREAM.md) for its source and attribution.
See [workspace commands](HACKING.md#workspace-commands) for build and test instructions.

# Fork Status

This repository is a maintained fork of
[erickt/rust-zmq](https://github.com/erickt/rust-zmq).

Why this fork exists:

- Upstream appears to have limited maintenance activity.
- There was significant backlog in open pull requests and issues.
- Downstream users needed fixes that were not available in a published release.

What has been integrated in this fork:

- Curve/libsodium support fixes and build integration.
- `zmq-sys` dependency updates and zeromq-src baseline updates.
- Selected non-breaking API improvements: `ZMQ_INVERT_MATCHING`,
  `ZMQ_TCP_MAXRT`, `set_xpub_verboser`, and `AsFd`/`AsSocket` support.
- 32-bit build compatibility and test/toolchain upkeep (bitflags 2.x, trybuild snapshots).
- Safety/correctness hardening: reject interior-NUL property names in `has`
  and `Message::gets`; guard pathological `Message` allocation sizes.
- Added fuzzing harness + scheduled ASAN smoke workflow.

This fork currently keeps crate package names unchanged (`zmq`, `zmq-sys`) to
minimize downstream migration cost.

If you need to force this fork in a workspace:

```toml
[patch.crates-io]
zmq = { git = "https://github.com/rdaum/rust-zmq.git", branch = "main" }
zmq-sys = { git = "https://github.com/rdaum/rust-zmq.git", branch = "main" }
```

# Compatibility

The aim of this project is to track latest zmq releases as close as possible.

Regarding the minimum Rust version required, `zmq` is CI-tested on current 
stable channels of Rust. 

# Usage

`zmq` is a pretty straight forward port of the C API into Rust:

```rust
fn main() {
    let ctx = zmq::Context::new();

    let socket = ctx.socket(zmq::REQ).unwrap();
    socket.connect("tcp://127.0.0.1:1234").unwrap();
    socket.send("hello world!", 0).unwrap();
}
```

You can find more usage examples in
[the synchronous examples](crates/zmq/examples) and [the async examples](crates/zmq-async/examples).

# Notes

## Process environment safety

`libzmq` may read process environment variables internally. Avoid mutating the
process environment (for example via `std::env::set_var`/`remove_var`) after
creating `zmq::Context` values or while other threads may be using `zmq`.

## Shutdown and linger

Sockets default to `ZMQ_LINGER = -1` (infinite), so dropping a socket can block
while pending outbound messages flush. For fast shutdown, set `socket.set_linger(0)`.

# Contributing

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the
Apache-2.0 license, shall be dual licensed under the terms of both the
Apache License, Version 2.0 and the MIT license without any additional
terms or conditions.

See the [contribution guidelines] for what to watch out for when
submitting a pull request.

[contribution guidelines]: ./CONTRIBUTING.md
