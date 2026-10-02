# Source provenance and licenses

This file records the source of the imported code and the license terms retained by this fork. The
review below covers the two imported Rust projects and their main native dependencies.

## Synchronous and FFI bindings

`crates/r0rz` and `crates/r0rz-sys` come from [erickt/rust-zmq](https://github.com/erickt/rust-zmq).
The fork includes upstream commit
[`5d78967001abb1aece2fba878d6151cb66cd1767`](https://github.com/erickt/rust-zmq/commit/5d78967001abb1aece2fba878d6151cb66cd1767),
dated May 30, 2025. The package versions at that source were `zmq` 0.10.0 and `zmq-sys` 0.12.0.

The upstream
[manifest](https://github.com/erickt/rust-zmq/blob/5d78967001abb1aece2fba878d6151cb66cd1767/Cargo.toml)
declares `MIT/Apache-2.0`. This workspace uses the equivalent SPDX expression, `MIT OR Apache-2.0`.
The original MIT copyright notice for Erick Tryzelaar and the Apache license text are retained in
both crates. The manifests retain the original author entries. Git history retains upstream commits
and attribution for imported fixes.

Fork changes include build updates, additional socket options, safety fixes, tests, workspace paths,
package names, and documentation. See [NEWS.md](NEWS.md) and Git history for individual changes.

## Async bindings

`crates/r0rz-async` comes from [cetra3/tmq](https://github.com/cetra3/tmq), version 0.5.0. The
import uses commit
[`538cce9b0fed9dd90a4bb3bcf28eb54e1f94973b`](https://github.com/cetra3/tmq/commit/538cce9b0fed9dd90a4bb3bcf28eb54e1f94973b).
The upstream
[manifest](https://github.com/cetra3/tmq/blob/538cce9b0fed9dd90a4bb3bcf28eb54e1f94973b/Cargo.toml)
declares `MIT/Apache-2.0` too. The imported source therefore has compatible license terms for this
workspace.

The manifest retains all five upstream author entries. The import includes source, tests, examples,
and a benchmark. See [the crate's origin record](crates/r0rz-async/UPSTREAM.md) for authors, license
text details, and changes made here.

## Native libraries

The Rust binding licenses do not replace the native library licenses.

| Component              | Role                                          | License and source                                                                                          |
| ---------------------- | --------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `zeromq-src`           | Rust build helper and bundled `libzmq` source | [Wrapper: MIT OR Apache-2.0](https://crates.io/crates/zeromq-src/0.3.6); bundled library has separate terms |
| `libzmq` 4.3.5         | Native ZeroMQ implementation                  | [MPL-2.0](https://github.com/zeromq/libzmq/blob/v4.3.5/LICENSE)                                             |
| `libsodium-sys-stable` | Rust build helper and FFI for libsodium       | [MIT OR Apache-2.0](https://crates.io/crates/libsodium-sys-stable)                                          |
| libsodium              | Native cryptography used for CURVE            | [ISC](https://github.com/jedisct1/libsodium/blob/1.0.20-RELEASE/LICENSE)                                    |

MPL-2.0 permits combining `libzmq` with separately licensed Rust bindings. It does not require
changing these bindings to MPL-2.0. Mozilla explains this in its
[license FAQ, questions 11 and 13](https://www.mozilla.org/en-US/MPL/2.0/FAQ/).

When distributing a binary that includes `libzmq`, provide access to the corresponding MPL-covered
source, including any changes. Retain its license and notices. The source requirement applies to the
MPL-covered files. See [MPL-2.0, sections 3.2 and 3.3](https://www.mozilla.org/en-US/MPL/2.0/).

The ISC license permits use and distribution with the copyright and permission notices retained.
Keep the native source distributions' additional notices for bundled components too. Use the
versions selected by your build when preparing those notices and source links.

The license declarations reviewed here have no conflict that prevents this workspace from combining
these components. Each dependency still retains its own license obligations.
