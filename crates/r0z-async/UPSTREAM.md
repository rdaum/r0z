# Async crate origin

This crate comes from [cetra3/tmq](https://github.com/cetra3/tmq), version `0.5.0`. The source
commit is
[`538cce9b0fed9dd90a4bb3bcf28eb54e1f94973b`](https://github.com/cetra3/tmq/commit/538cce9b0fed9dd90a4bb3bcf28eb54e1f94973b).

The upstream manifest lists these authors, retained in this crate's manifest:

- cetra3 <cetra3@hotmail.com>
- skrap <jonah@petri.us>
- kobzol <berykubik@gmail.com>
- YushiOMOTE <yushiomote@gmail.com>
- iddm <fx@thefx.co>

The upstream
[license declaration](https://github.com/cetra3/tmq/blob/538cce9b0fed9dd90a4bb3bcf28eb54e1f94973b/Cargo.toml)
is `MIT/Apache-2.0`. This workspace retains that choice as `MIT OR Apache-2.0`. The source snapshot
did not include separate license files. This fork supplies the standard [MIT](LICENSE-MIT) and
[Apache-2.0](LICENSE-APACHE) texts. The MIT file credits the tmq contributors; that attribution line
was added here. It does not attribute the async implementation to the rust-zmq authors.

The import includes source, tests, examples, and the benchmark. The original README is available at
the pinned upstream commit.

Changes made in this workspace:

- Renamed the package from `tmq` to `r0z-async` and updated imports.
- Moved dependency declarations into the workspace manifest and used the local `r0z` package.
- Added a module documentation marker and mechanical Clippy fixes in examples, tests, and the
  benchmark.
- Updated documentation and the issue reporting link for this fork.
- Added license texts and this origin record.

The initial import preserved socket behaviour. Later changes added runtime adapters for Tokio and
async-io, shared readiness handling, and tests across both adapters. Built-in adapters currently
support Unix. Request/reply operations now borrow a `RequestReply` socket, with explicit protocol
state and a buffered send that survives cancellation. Payload-copy improvements remain separate
work.
