# AGENTS.md

Quick-start context for coding agents working in this repository. Read
[CONTRIBUTING.md](./CONTRIBUTING.md) for contribution guidance and [HACKING.md](./HACKING.md) for
build commands. [PROVENANCE.md](./PROVENANCE.md) records upstream sources and licence terms.

## Project shape

r0z provides Rust bindings to native ZeroMQ (`libzmq`). It is not a ZeroMQ implementation written
from scratch in Rust. The synchronous and FFI crates descend from `rust-zmq`. The async crate
descends from `tmq`.

```text
crates/
├── r0z/         # safe synchronous bindings, examples, and tests
├── r0z-sys/     # native library build and unsafe FFI bindings
└── r0z-async/   # async sockets built on r0z, Tokio and async-io adapters on Unix
fuzz/           # separate Cargo workspace with cargo-fuzz targets
msrv-test/      # separate Cargo workspace with a minimal consumer
```

The workspace uses Cargo resolver version 2. Member crates use different Rust editions. Preserve
those editions unless the task includes an edition change. The `msrv-test` name alone does not
establish a tested minimum Rust version.

The native build uses `zeromq-src`. The synchronous crate enables libsodium for CURVE support. The
async crate supports Tokio and async-io adapters on Unix. Keep ZeroMQ logic shared and runtime
registration inside the adapters. The [README](./README.md#development) links to tickets for planned
work and reported defects.

## Human responsibility

Code written with an agent remains the responsibility of the human author.

- Keep changes small enough for the human to review, understand, and explain.
- Read the relevant implementation and tests before editing.
- Preserve unrelated work in a dirty tree.
- State uncertainty, incomplete validation, and known limitations.
- Keep plans, status updates, comments, and documentation brief and specific.
- Use simple English. Remove marketing prose, filler, and comments that merely restate code.
- Use Canadian English in documentation and comments. Preserve identifiers, quoted text, and
  upstream licence notices exactly.

## Permission boundaries

Only perform state-changing Git operations when the human explicitly requests them. This includes
staging, commits, amends, branch changes, merges, rebases, cherry-picks, resets, stashes, tags, and
pushes. Read-only inspection is allowed. A request to commit does not authorize a push or release.

Only publish crates, tags, releases, or remote messages when explicitly requested. Permission to
edit code does not authorize changes to downstream repositories, remote services, or host
configuration. Ordinary local builds, formatters, linters, and tests are allowed for the requested
work.

## Engineering policy

- Keep changes focused on the requested behaviour and the current architecture.
- Prefer one clear API over speculative abstractions or compatibility scaffolding. Account for real
  downstream callers when changing an existing API.
- Keep dependency versions in the root `Cargo.toml`. Workspace members use `workspace = true`. The
  standalone fuzz and consumer projects have their own manifests.
- Preserve native names such as `zmq_*`, `ZMQ_*`, and `links = "zmq"`. They describe the native ABI.
- Keep upstream authors, licence notices, and source provenance intact when importing code.
- Put detailed defect analysis and planned improvements in issues. Link to them from the README.
- Record notable completed changes in the current unreleased section of [NEWS.md](./NEWS.md).
- Measure performance changes on representative workloads before claiming an improvement.
- Keep message paths conscious of allocations, payload copies, and backpressure.

## Native and async correctness

The safe Rust API must uphold its guarantees across the native boundary.

- Explain pointer validity, lifetime, ownership, size, and thread-safety requirements for unsafe
  code.
- Give each native socket, message, and context a clear owner. Check cleanup on errors and
  cancellation.
- Preserve multipart frame order, message boundaries, and native REQ/REP sequencing rules.
- Treat the ZeroMQ file descriptor as an event notification source. Interpret readiness through
  `ZMQ_EVENTS`; do not treat it as an ordinary data socket.
- Review waker registration and readiness clearing together. Consider separate send and receive
  tasks, spurious readiness, and `EAGAIN` retries.
- Account for native linger and context shutdown. Keep hanging regression tests bounded, including
  cleanup. A timeout inside a blocked runtime is not a sufficient deadline.
- Do not change the process environment while native library threads can read it.
- Preserve platform-specific FFI details. Do not regenerate the whole binding file for a small edit.
- Distinguish upstream defect reports from failures reproduced in this fork.

## Formatting and verification

Use rustfmt for Rust. Use [dprint.json](./dprint.json) for Markdown, TOML, and JSON. Format files
touched by the task. Keep unrelated formatting cleanup separate.

```sh
cargo fmt --all -- --check
dprint check AGENTS.md dprint.json
cargo test --workspace --all-targets
cargo test --workspace --doc
cargo clippy --workspace --all-targets -- -D warnings
```

Replace the dprint file arguments with the documentation and configuration files changed by the
task. Use `dprint fmt <files>` to format them.

On Windows, add `--exclude r0z-async` to workspace test and Clippy commands. The async crate
currently requires Unix. Use `cargo test -p <package>` for focused tests.

When dependency names or workspace paths change, also check the standalone consumers:

```sh
cargo check --manifest-path msrv-test/Cargo.toml
cargo check --manifest-path fuzz/Cargo.toml --bins
```

Add regression tests for changed behaviour. Prefer real sockets over mocks when native behaviour
matters. Run the relevant tests and broader workspace checks for changes that cross crate
boundaries. For safety-sensitive changes, follow the fuzz guidance in
[CONTRIBUTING.md](./CONTRIBUTING.md). Documentation-only edits normally need formatting and link
checks.

Report what passed, what failed, and what was not run. Distinguish existing formatting or test
failures from failures caused by the change. Linux validation does not establish Windows or macOS
support.

## Commits

Only create commits when explicitly requested. Use Conventional Commits with a specific imperative
subject. For non-trivial changes, explain the motivation, resulting behaviour, and relevant
validation in the body. Stage only the requested work. Do not amend a commit that was already
pushed.
