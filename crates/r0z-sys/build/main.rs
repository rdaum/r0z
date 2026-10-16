extern crate zeromq_src;

#[cfg(feature = "libsodium")]
extern crate cc;
#[cfg(feature = "libsodium")]
extern crate libflate;
#[cfg(feature = "libsodium")]
extern crate minisign_verify;
#[cfg(feature = "libsodium")]
extern crate pkg_config;
#[cfg(feature = "libsodium")]
extern crate tar;
#[cfg(feature = "libsodium")]
extern crate tempfile;
#[cfg(feature = "libsodium")]
extern crate ureq;
#[cfg(feature = "libsodium")]
extern crate vcpkg;
#[cfg(all(feature = "libsodium", windows))]
extern crate zip;

use std::env;

#[cfg(feature = "libsodium")]
mod sodium;
#[cfg(feature = "libsodium")]
mod sodium_compile;

pub fn configure() {
    println!("cargo:rerun-if-changed=build");
    println!("cargo:rerun-if-env-changed=PROFILE");

    // libzmq's Windows code path references security descriptor APIs.
    // Ensure final linkage includes the import library that provides them.
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!("cargo:rustc-link-lib=advapi32");
    }

    #[cfg(feature = "libsodium")]
    let sodium_paths = Some(sodium::build());
    #[cfg(not(feature = "libsodium"))]
    let sodium_paths = None;

    // zeromq-src enables CURVE only when it receives sodium paths.
    zeromq_src::Build::new()
        .with_libsodium(sodium_paths)
        .build();
}

fn main() {
    configure()
}
