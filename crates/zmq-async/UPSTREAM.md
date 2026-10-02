# Async crate origin

This crate comes from [cetra3/tmq](https://github.com/cetra3/tmq), version `0.5.0`.
The source commit is [`538cce9b0fed9dd90a4bb3bcf28eb54e1f94973b`](https://github.com/cetra3/tmq/commit/538cce9b0fed9dd90a4bb3bcf28eb54e1f94973b).

The import includes the source, tests, examples, benchmark, and upstream README.
The manifest retains the upstream authors and the MIT OR Apache-2.0 license declaration.
Dependencies use the workspace manifest, including the local `zmq` package.
The package retains the name `tmq`, with publication disabled until a new name is selected.

This initial import retains the Tokio implementation and its Unix requirement.
Runtime adapters and changes to socket behavior belong to subsequent work.

Import adjustments include a module documentation marker and mechanical Clippy fixes in examples, tests, and the benchmark.
These changes preserve socket behavior.
