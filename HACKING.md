# Building and testing r0z

r0z provides Rust bindings to native ZeroMQ. The repository contains three crates:

- `r0z`: the synchronous Rust API.
- `r0z-async`: the async Rust API, with support for Tokio, async-io, and compio on Unix.
- `r0z-sys`: the native library build and low-level C bindings.

Use current stable Rust and a C/C++ build toolchain. Linux and macOS builds also need `make`. Cargo
builds the native ZeroMQ library from source.

Run the commands below from the repository root. See the [contribution guidelines] before submitting
changes.

[contribution guidelines]: ./CONTRIBUTING.md

## Build and test

On Linux or macOS:

```sh
cargo build --workspace --all-targets
cargo test --workspace --all-targets
cargo test --workspace --doc
cargo clippy --workspace --all-targets -- -D warnings
```

On Windows, exclude `r0z-async`, which currently requires Unix:

```sh
cargo build --workspace --exclude r0z-async --all-targets
cargo test --workspace --exclude r0z-async --all-targets
cargo test --workspace --exclude r0z-async --doc
cargo clippy --workspace --exclude r0z-async --all-targets -- -D warnings
```

To test one crate, use `cargo test -p r0z`, `cargo test -p r0z-sys`, or `cargo test -p r0z-async`.
The `fuzz/` and `msrv-test/` directories are separate Cargo projects. The first contains fuzz tests;
the second is a small program that checks use of `r0z` from another crate.

## Test with other async runtimes

The default tests use Tokio. When changing async code, also run the tests with async-io and compio:

```sh
cargo test -p r0z-async --no-default-features --features async-io --all-targets
cargo test -p r0z-async --no-default-features --features compio --all-targets
cargo test -p r0z-async --all-features --all-targets
cargo test -p r0z-async --all-features --doc
cargo clippy -p r0z-async --all-features --all-targets -- -D warnings
```

With all features enabled, each shared socket test runs once per runtime. Also check that the
library builds without a built-in runtime:

```sh
cargo check -p r0z-async --no-default-features --lib
```

The examples show how to create sockets and exchange messages with each runtime:

```sh
cargo run -p r0z-async --no-default-features --features async-io --example async_io
cargo run -p r0z-async --no-default-features --features compio --example compio
```

### Choose a compio driver

On Linux, compio can wait for socket events through io_uring or its polling driver. Test both with:

```sh
R0Z_COMPIO_DRIVER=io-uring cargo test -p r0z-async --no-default-features --features compio --all-targets
R0Z_COMPIO_DRIVER=poll cargo test -p r0z-async --no-default-features --features compio --all-targets
```

The first command fails if the system does not support or permit io_uring. macOS uses the polling
driver. `R0Z_COMPIO_DRIVER` only selects the driver for these tests, not for applications using r0z.

## Investigate a hanging test

A separate timer thread stops each shared async test after 60 seconds. This limit includes all
enabled runtimes and the time spent closing sockets and shutting down.

Request and load tests print progress every five seconds. Reports show the runtime, socket address,
completed message counts, and each side's last operation. Errors from the other side of a connection
can explain why a sender or receiver is waiting. These reports remain visible after an abort, even
when Cargo hides ordinary test output.

For request/reply failures, run:

```sh
cargo test -p r0z-async --all-features --test request --test watchdog
```

The `watchdog` tests check that failure reports survive an abort. For tests that send and receive
from separate tasks, run:

```sh
cargo test -p r0z-async --all-features --test dealer_split
```

ZeroMQ's linger setting controls how long shutdown waits for queued messages to reach their
recipients. To check timeouts and shutdown with different linger settings, run:

```sh
cargo test -p r0z-async --all-features --test timeout_shutdown -- --nocapture
```

These tests run in separate processes with ten-second deadlines. Some deliberately select infinite
linger and leave messages undelivered. The test runner checks that shutdown blocks, then kills the
child process.

## Run larger receive tests

The normal receive tests exchange 10,000 messages. The million-message tests are excluded from
normal runs. To run them explicitly:

```sh
cargo test -p r0z-async --all-features hammer_stress -- --ignored
```

To run only the DEALER socket test:

```sh
cargo test -p r0z-async --all-features --test dealer receive_hammer_stress -- --ignored --exact
```

These tests use the same 60-second deadline and progress reports as the smaller tests.

## Building with encryption support

CURVE is ZeroMQ's encrypted transport. It uses libsodium, a native cryptography library. Both `r0z`
and `r0z-async` enable their `curve` feature by default. This feature enables `r0z-sys/libsodium`.
Disable default features to omit CURVE and sodium; select an async runtime adapter explicitly if
required. Cargo combines dependency features, so another dependency can still enable sodium.

Linux and macOS builds compile the signed source archive in `crates/r0z-sys/vendor/libsodium`. These
builds require a C compiler, a C++ compiler, and `make`. They do not download native source. If the
target directory contains spaces, the build uses a temporary directory and copies the results back.
The temporary directory must have a path without spaces.

Windows builds download signed libsodium 1.0.22 stable archives over HTTPS. MSVC targets use ZIP
archives; GNU targets use tar archives. The build checks the upstream Minisign signature before
extraction and caches verified downloads under `OUT_DIR`. Upstream stable archive URLs are mutable.
For reproducible or offline Windows builds, retain an archive and its signature in
`SODIUM_DIST_DIR`. Supported archives cover x86, x64, and ARM64 with MSVC, and x86 and x64 with GNU.

Only Windows hosts build the Rust `zip` dependency. Cross-compilation from Linux or macOS to Windows
MSVC requires `unzip` to extract the verified archive. The build regression tests also require
`unzip` on these hosts.

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

The build requires a static libsodium library. `SODIUM_SHARED` and dynamic vcpkg configurations are
unsupported. Missing libraries or headers stop the build instead of disabling CURVE. The build uses
compiler selection from `cc`, including target-specific `CC`, `AR`, and `RANLIB`.

Run the build regressions and CURVE key and message tests with:

```sh
cargo test -p r0z-sys --features libsodium --test sodium_build
cargo test -p r0z --test curve
cargo check -p r0z-sys --no-default-features
cargo test -p r0z --no-default-features --all-targets
cargo test -p r0z-async --no-default-features --features tokio,async-io --all-targets
```

With the `curve` feature enabled, the encryption tests fail if the compiled ZeroMQ library lacks
CURVE support. Without this feature, these tests are disabled. Use a fresh target directory to check
native build changes on each target platform.

## Benchmarks

To measure how long message delivery takes and how many messages can be sent per second, run:

```sh
cargo bench -p r0z-async --bench owned_send
```

The benchmark uses Tokio with connections inside one process and over TCP. It covers small frames,
large frames, and multipart messages. Payload preparation is outside the timed region. Each sample
waits for peer receipt.

To check the scripts that compare r0z with the tmq and OMQ Rust libraries, run:

```sh
python3 benchmarks/comparison/run.py --smoke
```

This short run checks that the benchmark works. Use an otherwise idle machine for performance
measurements.

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

## Regenerating the C bindings

The `r0z-sys` crate contains the unsafe C bindings. Keep the platform-specific edits in [`ffi.rs`]
when regenerating its bindings from `zmq.h`. Replace the header path below with the path in your
ZeroMQ source tree:

```sh
bindgen \
   --with-derive-default \
   --whitelist-function "^zmq_.*" \
   --whitelist-type "^zmq_.*" \
   --whitelist-var "^ZMQ_.*" ~/src/zeromq-4.1.6/include/zmq.h
   > crates/r0z-sys/src/ffi.rs
```

[`ffi.rs`]: ./crates/r0z-sys/src/ffi.rs
