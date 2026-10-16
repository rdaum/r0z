# Sodium source and build-code provenance

`libsodium.tar.gz` and `libsodium.tar.gz.minisig` are unchanged copies of `LATEST.tar.gz` and its
signature from the published `libsodium-sys-stable` 1.24.3 crate. The archive contains libsodium
1.0.22 stable source. Its upstream signature timestamp is 1791934505.

- Source crate: <https://crates.io/crates/libsodium-sys-stable/1.24.3>
- Source crate SHA-256: `1f55c1c508d31e2cf0a649c9c889e4bcf554cbfb2a6803f049f219784b044283`
- Source archive SHA-256: `eb9873cc5ccd7b26ef058394264663c99e9c047b423be1ffd484071774198f26`
- Upstream Minisign public key: `RWQf6LRCGA9i53mlYecO4IzT51TGPpvWucNSCh1CBM0QTaLn73Y7GFO3`

Every source build checks the signature before extraction. The archive includes the full source,
build files, and upstream notices. `LICENSE` is an unchanged copy of the native ISC licence.

The private build modules adapt `libsodium-sys-stable` 1.24.3 code by dnaq and jedisct1.
`BUILD-LICENSE-MIT` and `BUILD-LICENSE-APACHE` retain its licences and sodiumoxide copyright notice.
The private module does not change the cryptographic implementation or export Rust sodium bindings.

Windows builds retain upstream signed archive downloads. Their URLs identify the mutable 1.0.22
stable series, not a pinned snapshot. The build verifies each archive before extraction and caches
it under `OUT_DIR`. These binaries are not included in this crate. To reproduce a Windows build,
retain both the archive and its signature, then set `SODIUM_DIST_DIR` to their directory.

To update the bundled source, copy the archive and signature from a reviewed upstream release.
Update this record with the source, checksum, and signature timestamp. Retain the upstream licences.
Run the native build regressions and CURVE tests with a fresh Cargo target directory.
