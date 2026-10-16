# Hacking on r0z

This file is intended to collect useful knowledge when hacking on the r0z crate.

If you intend to provide a contribution, please check the [contribution guidelines].

[contribution guidelines]: ./CONTRIBUTING.md

## Workspace commands

On Linux or macOS, run these commands from the repository root:

```sh
cargo build --workspace --all-targets
cargo test --workspace --all-targets
cargo test --workspace --doc
cargo clippy --workspace --all-targets -- -D warnings
```

On Windows, exclude the async package because its current implementation requires Unix:

```sh
cargo build --workspace --exclude r0z-async --all-targets
cargo test --workspace --exclude r0z-async --all-targets
cargo test --workspace --exclude r0z-async --doc
cargo clippy --workspace --exclude r0z-async --all-targets -- -D warnings
```

For a single package, use `cargo test -p r0z`, `cargo test -p r0z-sys`, or
`cargo test -p r0z-async`. The `fuzz/` harness and `msrv-test/` consumer remain outside the main
workspace. Both depend on `crates/r0z` through a local path.

## Sodium builds

Both public crates enable their `curve` feature by default. This feature enables
`r0z-sys/libsodium`. Disable default features to omit CURVE and sodium; select an async runtime
adapter explicitly if required. Cargo combines dependency features, so another dependency can still
enable sodium.

The private native build module provides sodium to `zeromq-src`. It does not expose a Rust
cryptography API. Without `r0z-sys/libsodium`, ZeroMQ builds without CURVE.

Linux and macOS builds compile the signed source archive in `crates/r0z-sys/vendor/libsodium`. These
builds require a C compiler, a C++ compiler, and `make`. They do not download native source. If the
target directory contains spaces, the build uses a temporary directory and copies the results back.
The temporary directory must have a path without spaces.

Windows builds download signed libsodium 1.0.22 stable archives over HTTPS. MSVC targets use ZIP
archives; GNU targets use tar archives. The build checks the upstream Minisign signature before
extraction and caches verified downloads under `OUT_DIR`. Upstream stable archive URLs are mutable.
For reproducible or offline Windows builds, retain an archive and its signature in
`SODIUM_DIST_DIR`. Supported archives cover x86, x64, and ARM64 with MSVC, and x86 and x64 with GNU.

The native build accepts these environment variables:

| Variable                | Behaviour                                                                                             |
| ----------------------- | ----------------------------------------------------------------------------------------------------- |
| `SODIUM_LIB_DIR`        | Directory containing `libsodium.a`, or `libsodium.lib` for MSVC. Requires `SODIUM_INCLUDE_DIR`.       |
| `SODIUM_INCLUDE_DIR`    | Directory containing `sodium.h` and the `sodium/` headers. Requires `SODIUM_LIB_DIR`.                 |
| `SODIUM_USE_PKG_CONFIG` | Discover static sodium through pkg-config, with vcpkg as a fallback. Cannot accompany explicit paths. |
| `SODIUM_DIST_DIR`       | Read signed archives from this directory instead of bundled source or Windows downloads.              |
| `SODIUM_DISABLE_PIE`    | Pass `--disable-pie` to the Unix configure script.                                                    |
| `SODIUM_LDFLAGS`        | Additional linker flags for the Unix configure script.                                                |

`SODIUM_DIST_DIR` uses `LATEST.tar.gz` for Unix source, `libsodium-1.0.22-stable-msvc.zip` for MSVC,
or `libsodium-1.0.22-stable-mingw.tar.gz` for GNU Windows. Each archive needs a matching `.minisig`
file. Source archives must contain the `libsodium-stable/` directory.

`zeromq-src` emits static sodium link directives. `SODIUM_SHARED` and dynamic vcpkg configurations
are therefore unsupported. Missing libraries or headers stop the build instead of disabling CURVE.
The build uses compiler selection from `cc`, including target-specific `CC`, `AR`, and `RANLIB`.

Run the build regressions and CURVE key and message tests with:

```sh
cargo test -p r0z-sys --features libsodium --test sodium_build
cargo test -p r0z --test curve
cargo check -p r0z-sys --no-default-features
cargo test -p r0z --no-default-features --all-targets
cargo test -p r0z-async --no-default-features --features tokio,async-io --all-targets
```

With the `curve` feature enabled, the CURVE suite requires `r0z::has("curve") == Some(true)`. It
cannot skip absent native support. Without this feature, the CURVE suite is disabled. Use a fresh
target directory to check native build changes. Linux results do not establish macOS, Windows, or
cross-compilation support.

## Async runtime adapters

The default workspace commands cover Tokio. Run the same async socket tests with each feature
combination:

```sh
cargo check -p r0z-async --no-default-features --lib
cargo test -p r0z-async --no-default-features --features async-io --all-targets
cargo test -p r0z-async --all-features --all-targets
cargo test -p r0z-async --no-default-features --features async-io --doc
cargo clippy -p r0z-async --all-features --all-targets -- -D warnings
cargo run -p r0z-async --no-default-features --features async-io --example async_io
```

With both features enabled, each shared integration test body runs once per adapter. The async-io
tests use its reactor and an `async-executor` executor. The Tokio tests use its current-thread
runtime. A watchdog thread bounds each shared test, including native shutdown, to 60 seconds. The CI
matrix covers the core alone, each built-in adapter, and both adapters on Linux and macOS. Windows
CI covers the synchronous and FFI crates only.

## Receive stress diagnostics

Normal receive load tests send 10,000 multipart messages per adapter. The million-message versions
are ignored by default. Both versions use the same content assertions, diagnostics, and 60-second
deadline.

To run all million-message receive stress tests, use:

```sh
cargo test -p r0z-async --all-features hammer_stress -- --ignored
```

To run only the million-message DEALER test, use:

```sh
cargo test -p r0z-async --all-features --test dealer receive_hammer_stress -- --ignored --exact
```

The stress tests report the test source, active adapter, elapsed time, and sender and receiver
counts. Reports also identify socket/context drops, sender joins, and runtime shutdown. Counters
update after each complete multipart. Progress logs appear after the first message, every 100,000
messages, and at completion. The watchdog adds a snapshot every five seconds while progress tracking
is active.

Diagnostics write directly to stderr, so they survive a process abort with test capture enabled. The
watchdog still aborts after 60 seconds for the whole shared test, including both adapters when
enabled. Progress does not extend the deadline. Compare successive snapshots to distinguish slow
traffic from a stall, then use the phase to locate blocked shutdown. Adapter CI jobs finish
independently so one failure does not cancel the remaining combinations.

## Split DEALER tests

Run the split socket regressions with:

```sh
cargo test -p r0z-async --all-features --test dealer_split
```

The suite covers both bind directions over IPC and TCP. Each arrangement uses an unsplit socket,
split halves in one task, or split halves in separate tasks. Separate tests use small inproc queues
and require an observed blocked send before the peer reads.

The suite also covers task cancellation, resuming cancelled operations in new tasks, and idle poll
counts. Both adapters run under the shared 60-second watchdog. These tests run in normal CI.

## Receive timeout and shutdown tests

Run the isolated timeout cases with:

```sh
cargo test -p r0z-async --all-features --test timeout_shutdown -- --nocapture
```

The suite compares IPC and TCP, delivered and queued requests, and default and explicit linger
settings. It uses Tokio timeout and `select!`, plus async-io timers. Each child process has a
ten-second deadline. Failures include the adapter, transport, case, and last recorded phase.

Cases that explicitly select infinite linger with undelivered requests must block native shutdown.
The parent observes that phase for 200 milliseconds, then kills and reaps the child. Other cases
must return from the receive timeout and finish shutdown. These tests also run in the normal adapter
CI matrix. They do not establish the cause of the incomplete upstream report.

## Owned-message send benchmark

Run the latency and batch throughput measurements with:

```sh
cargo bench -p r0z-async --bench owned_send
```

The benchmark uses Tokio with inproc and TCP sockets. It covers small frames, large frames, and
multipart messages. Payload preparation is outside the timed region. Each sample waits for peer
receipt, so it measures delivery rather than queue insertion alone.

## Cross-library benchmarks

The TCP comparison harness uses common measurement code for r0z, tmq, and OMQ. Run a small
validation pass with:

```sh
python3 benchmarks/comparison/run.py --smoke
```

Use a quiet host for performance comparisons. Smoke runs validate the harness only.

## Release checks

All three packages use the version in the workspace manifest. The internal dependency versions must
match that release. Cargo publishes workspace dependencies before the packages that use them.

Before publication, verify the packages without an upload:

```sh
cargo publish --workspace --dry-run --all-features
```

This command builds the packaged sources and checks their registry dependencies. Inspect the
archives in `target/package` for source files, license files, and README content. Linux CI runs the
same command. For local preparation with uncommitted edits, add `--allow-dirty` to the dry run.

After publication, update downstream Git dependencies to the published versions.

## Regenerating the C bindings

The C low-level, unsafe C binding is provided by the `r0z-sys` crate, also hosted in this
repository. The bindings are only partially auto-generated, to allow getting some platform-specific
details right.

The autogenerated part produced by the `bindgen` crate is stored in the [`ffi.rs`] file inside the
`r0z-sys` crate, and is created using the `zmq.h` header file; using the following command:

```sh
bindgen \
   --with-derive-default \
   --whitelist-function "^zmq_.*" \
   --whitelist-type "^zmq_.*" \
   --whitelist-var "^ZMQ_.*" ~/src/zeromq-4.1.6/include/zmq.h
   > crates/r0z-sys/src/ffi.rs
```

[`ffi.rs`]: ./crates/r0z-sys/src/ffi.rs
