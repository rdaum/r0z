// Native build support adapted from libsodium-sys-stable 1.24.3 (dnaq, jedisct1).
// Copyright (c) 2015 The sodiumoxide Developers
// Licensed under MIT OR Apache-2.0; see vendor/libsodium/BUILD-LICENSE-*.

use std::{
    env, fs,
    io::{Cursor, Read},
    path::{Path, PathBuf},
    time::Duration,
};

use libflate::gzip::Decoder;
use minisign_verify::{PublicKey, Signature};
use zeromq_src::LibLocation;
#[cfg(windows)]
use zip;
use {pkg_config, sodium_compile, tar, tempfile, ureq, vcpkg};

const PUBLIC_KEY: &str = "RWQf6LRCGA9i53mlYecO4IzT51TGPpvWucNSCh1CBM0QTaLn73Y7GFO3";
const VENDOR: &str = "vendor/libsodium";

pub fn build() -> LibLocation {
    for name in &[
        "SODIUM_LIB_DIR",
        "SODIUM_INCLUDE_DIR",
        "SODIUM_SHARED",
        "SODIUM_USE_PKG_CONFIG",
        "SODIUM_DIST_DIR",
        "SODIUM_DISABLE_PIE",
        "SODIUM_LDFLAGS",
        "VCPKGRS_DYNAMIC",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
    }
    println!("cargo:rerun-if-changed={VENDOR}");

    // zeromq-src emits static sodium link directives itself.
    assert!(
        env::var_os("SODIUM_SHARED").is_none(),
        "r0z-sys requires static libsodium; unset SODIUM_SHARED"
    );
    let target = env::var("TARGET").unwrap();
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let explicit = env::var_os("SODIUM_LIB_DIR");
    let include = env::var_os("SODIUM_INCLUDE_DIR");
    let discover = env::var_os("SODIUM_USE_PKG_CONFIG").is_some();
    assert!(
        !(discover && (explicit.is_some() || include.is_some())),
        "SODIUM_USE_PKG_CONFIG cannot be combined with explicit sodium paths"
    );
    let external = explicit.is_some() || discover;
    let (lib, include) = if let Some(lib) = explicit {
        let include = include.expect("SODIUM_LIB_DIR requires SODIUM_INCLUDE_DIR");
        (PathBuf::from(lib), PathBuf::from(include))
    } else if discover {
        find_system(&target)
    } else {
        assert!(
            include.is_none(),
            "SODIUM_INCLUDE_DIR requires SODIUM_LIB_DIR"
        );
        if target.contains("windows") {
            windows(&target, &out)
        } else {
            bundled(&target, &out)
        }
    };
    require_paths(&target, &lib, &include);
    // Watching generated artifacts would rebuild sodium on every Cargo run.
    if external {
        println!("cargo:rerun-if-changed={}", lib.display());
        println!("cargo:rerun-if-changed={}", include.display());
    }
    let include = if target.contains("msvc") {
        stage_msvc_headers(&include, &out)
    } else {
        include
    };
    LibLocation::new(lib, include)
}

fn require_paths(target: &str, lib: &Path, include: &Path) {
    let library = if target.contains("msvc") {
        "libsodium.lib"
    } else {
        "libsodium.a"
    };
    assert!(
        lib.join(library).is_file(),
        "Missing static sodium library: {}",
        lib.join(library).display()
    );
    assert!(
        include.join("sodium.h").is_file(),
        "Missing sodium.h in {}",
        include.display()
    );
    assert!(
        include.join("sodium/version.h").is_file(),
        "Missing sodium/version.h in {}",
        include.display()
    );
}

fn find_system(target: &str) -> (PathBuf, PathBuf) {
    if let Ok(lib) = pkg_config::Config::new()
        .statik(true)
        .cargo_metadata(false)
        .probe("libsodium")
    {
        let library = if target.contains("msvc") {
            "libsodium.lib"
        } else {
            "libsodium.a"
        };
        let path = lib
            .link_paths
            .iter()
            .find(|p| p.join(library).is_file())
            .expect("pkg-config did not provide a static sodium library")
            .clone();
        let include = lib
            .include_paths
            .iter()
            .find(|p| p.join("sodium.h").is_file())
            .expect("pkg-config did not provide sodium.h")
            .clone();
        // Emit transitive system libraries as well as sodium's own link metadata.
        pkg_config::Config::new()
            .statik(true)
            .probe("libsodium")
            .unwrap();
        return (path, include);
    }
    assert!(
        env::var_os("VCPKGRS_DYNAMIC").is_none(),
        "r0z-sys requires static vcpkg libraries"
    );
    let lib = vcpkg::Config::new()
        .find_package("libsodium")
        .expect("Static libsodium not found through pkg-config or vcpkg");
    let path = lib
        .link_paths
        .iter()
        .find(|p| p.join("libsodium.lib").is_file())
        .expect("vcpkg did not provide libsodium.lib")
        .clone();
    let include = lib
        .include_paths
        .iter()
        .find(|p| p.join("sodium.h").is_file())
        .expect("vcpkg did not provide sodium.h")
        .clone();
    (path, include)
}

fn verify(archive: Vec<u8>, signature: &str) -> Vec<u8> {
    PublicKey::from_base64(PUBLIC_KEY)
        .unwrap()
        .verify(
            &archive,
            &Signature::decode(signature).expect("Invalid sodium signature encoding"),
            false,
        )
        .expect("Invalid sodium archive signature");
    archive
}

fn local_archive(directory: &Path, name: &str) -> Vec<u8> {
    let archive = directory.join(name);
    let signature = directory.join(format!("{name}.minisig"));
    println!("cargo:rerun-if-changed={}", archive.display());
    println!("cargo:rerun-if-changed={}", signature.display());
    verify(
        fs::read(archive).expect("Cannot read sodium archive"),
        &fs::read_to_string(signature).expect("Cannot read sodium signature"),
    )
}

fn unpack_tar(bytes: Vec<u8>, destination: &Path) {
    let decoder = Decoder::new(Cursor::new(bytes)).expect("Invalid sodium gzip archive");
    tar::Archive::new(decoder)
        .unpack(destination)
        .expect("Cannot extract sodium archive");
}

fn bundled(target: &str, out: &Path) -> (PathBuf, PathBuf) {
    let bytes = match env::var_os("SODIUM_DIST_DIR") {
        Some(directory) => local_archive(Path::new(&directory), "LATEST.tar.gz"),
        None => local_archive(Path::new(VENDOR), "libsodium.tar.gz"),
    };
    // Autoconf/libtool cannot reliably build in paths containing spaces.
    // Use a unique scratch directory, then retain the artifacts under OUT_DIR.
    let scratch = if out.to_string_lossy().contains(' ') {
        Some(tempfile::tempdir().expect("Cannot create sodium build directory"))
    } else {
        None
    };
    let build_dir = scratch.as_ref().map_or(out, |dir| dir.path());
    assert!(
        !build_dir.to_string_lossy().contains(' '),
        "libsodium needs TMPDIR or CARGO_TARGET_DIR without spaces"
    );
    let source = build_dir.join("sodium-source");
    if source.exists() {
        fs::remove_dir_all(&source).expect("Cannot clear the previous sodium build");
    }
    fs::create_dir_all(&source).unwrap();
    unpack_tar(bytes, &source);
    let install = build_dir.join("sodium-installed");
    if install.exists() {
        fs::remove_dir_all(&install).unwrap();
    }
    fs::create_dir_all(&install).unwrap();
    let mut configure_target = target.to_owned();
    if target.starts_with("riscv") {
        let (_, suffix) = target.split_once('-').unwrap();
        configure_target = format!("riscv{}-{suffix}", &target[5..7]);
    }
    let lib = sodium_compile::compile(
        &configure_target,
        &source.join("libsodium-stable"),
        &install,
    )
    .unwrap_or_else(|error| panic!("Cannot build libsodium: {}", error));
    if scratch.is_some() {
        let retained = out.join("sodium-installed");
        if retained.exists() {
            fs::remove_dir_all(&retained).unwrap();
        }
        fs::create_dir_all(retained.join("lib")).unwrap();
        fs::copy(lib.join("libsodium.a"), retained.join("lib/libsodium.a")).unwrap();
        copy_headers(&install.join("include"), &retained.join("include"));
        (retained.join("lib"), retained.join("include"))
    } else {
        (lib, install.join("include"))
    }
}

fn windows(target: &str, out: &Path) -> (PathBuf, PathBuf) {
    let msvc = target.ends_with("-msvc");
    let arch = match target {
        "i686-pc-windows-msvc" => "Win32",
        "x86_64-pc-windows-msvc" => "x64",
        "aarch64-pc-windows-msvc" => "ARM64",
        "i686-pc-windows-gnu" => "libsodium-win32",
        "x86_64-pc-windows-gnu" => "libsodium-win64",
        _ => panic!(
            "No bundled sodium archive for target {}; set SODIUM_LIB_DIR and SODIUM_INCLUDE_DIR",
            target
        ),
    };
    let name = if msvc {
        "libsodium-1.0.22-stable-msvc.zip"
    } else {
        "libsodium-1.0.22-stable-mingw.tar.gz"
    };
    let bytes = if let Some(directory) = env::var_os("SODIUM_DIST_DIR") {
        local_archive(Path::new(&directory), name)
    } else {
        let cache = out.join("sodium-download");
        fs::create_dir_all(&cache).unwrap();
        if cache.join(name).is_file() && cache.join(format!("{name}.minisig")).is_file() {
            local_archive(&cache, name)
        } else {
            let bytes = download(name);
            let signature = String::from_utf8(download(&format!("{name}.minisig"))).unwrap();
            let bytes = verify(bytes, &signature);
            fs::write(cache.join(name), &bytes).unwrap();
            fs::write(cache.join(format!("{name}.minisig")), signature).unwrap();
            bytes
        }
    };
    let install = out.join("sodium-windows");
    if install.exists() {
        fs::remove_dir_all(&install).unwrap();
    }
    fs::create_dir_all(&install).unwrap();
    if msvc {
        unpack_msvc_zip(bytes, &install);
        let profile = if env::var("PROFILE").unwrap() == "release" {
            "Release"
        } else {
            "Debug"
        };
        (
            install.join(format!("libsodium/{arch}/{profile}/v143/static")),
            install.join("libsodium/include"),
        )
    } else {
        unpack_tar(bytes, &install);
        (
            install.join(arch).join("lib"),
            install.join(arch).join("include"),
        )
    }
}

#[cfg(windows)]
fn unpack_msvc_zip(bytes: Vec<u8>, destination: &Path) {
    zip::ZipArchive::new(Cursor::new(bytes))
        .expect("Invalid sodium ZIP archive")
        .extract(destination)
        .expect("Cannot extract sodium ZIP archive");
}

#[cfg(not(windows))]
fn unpack_msvc_zip(bytes: Vec<u8>, destination: &Path) {
    // Build scripts run on the host. Keep Unix-to-MSVC builds working without a Rust ZIP dependency.
    let temp = tempfile::tempdir().expect("Cannot create sodium archive directory");
    let archive = temp.path().join("sodium.zip");
    fs::write(&archive, bytes).unwrap();
    let output = std::process::Command::new("unzip")
        .arg("-q")
        .arg(&archive)
        .arg("-d")
        .arg(destination)
        .output()
        .expect("Extracting sodium for a Windows MSVC target requires unzip on non-Windows hosts");
    assert!(
        output.status.success(),
        "Cannot extract sodium ZIP archive: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn download(name: &str) -> Vec<u8> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(300)))
        .proxy(ureq::Proxy::try_from_env())
        .build()
        .new_agent();
    let mut response = agent
        .get(&format!(
            "https://download.libsodium.org/libsodium/releases/{name}"
        ))
        .call()
        .expect("Cannot download sodium; use SODIUM_DIST_DIR for offline builds");
    let mut bytes = Vec::new();
    response
        .body_mut()
        .as_reader()
        .take(64 * 1024 * 1024)
        .read_to_end(&mut bytes)
        .unwrap();
    bytes
}

fn stage_msvc_headers(include: &Path, out: &Path) -> PathBuf {
    // zeromq-src 0.3.6 copies ../../../builds/msvc/version.h into the include
    // directory. Stage a private copy so installed headers stay untouched.
    let root = out.join("sodium-headers");
    let staged = root.join("src/libsodium/include");
    copy_headers(include, &staged);
    // zeromq-src links static sodium but does not define SODIUM_STATIC for its C++ build.
    // Select static declarations in the private export header to avoid DLL import symbols.
    let exports = staged.join("sodium/export.h");
    let original = fs::read_to_string(&exports).unwrap();
    fs::write(
        exports,
        format!("#ifndef SODIUM_STATIC\n#define SODIUM_STATIC\n#endif\n{original}"),
    )
    .unwrap();
    let msvc = root.join("builds/msvc");
    fs::create_dir_all(&msvc).unwrap();
    fs::copy(include.join("sodium/version.h"), msvc.join("version.h")).unwrap();
    staged
}

fn copy_headers(include: &Path, destination: &Path) {
    fs::create_dir_all(destination.join("sodium")).unwrap();
    fs::copy(include.join("sodium.h"), destination.join("sodium.h")).unwrap();
    for entry in fs::read_dir(include.join("sodium")).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            fs::copy(
                entry.path(),
                destination.join("sodium").join(entry.file_name()),
            )
            .unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile;

    #[test]
    fn msvc_zip_extracts_to_a_path_with_spaces() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("sodium headers");
        fs::create_dir_all(&destination).unwrap();
        unpack_msvc_zip(
            include_bytes!("../tests/fixtures/sodium-headers.zip").to_vec(),
            &destination,
        );
        assert_eq!(
            fs::read(destination.join("libsodium/include/sodium.h")).unwrap(),
            b"test sodium header\n"
        );
    }

    #[test]
    fn bundled_source_has_a_valid_signature() {
        let bytes = local_archive(Path::new(VENDOR), "libsodium.tar.gz");
        let temp = tempfile::tempdir().unwrap();
        unpack_tar(bytes, temp.path());
        assert!(temp.path().join("libsodium-stable/configure").is_file());
        assert!(temp.path().join("libsodium-stable/LICENSE").is_file());
    }

    #[test]
    #[should_panic(expected = "Invalid sodium archive signature")]
    fn corrupt_source_is_rejected_before_extraction() {
        let mut bytes = fs::read(Path::new(VENDOR).join("libsodium.tar.gz")).unwrap();
        bytes[100] ^= 1;
        let signature =
            fs::read_to_string(Path::new(VENDOR).join("libsodium.tar.gz.minisig")).unwrap();
        verify(bytes, &signature);
    }

    #[test]
    #[should_panic(expected = "Missing static sodium library")]
    fn missing_library_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        require_paths("x86_64-unknown-linux-gnu", temp.path(), temp.path());
    }

    #[test]
    #[should_panic(expected = "Missing sodium.h")]
    fn missing_headers_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("libsodium.a"), []).unwrap();
        require_paths("x86_64-unknown-linux-gnu", temp.path(), temp.path());
    }

    #[test]
    fn msvc_headers_accommodate_zeromq_without_modifying_the_installation() {
        let temp = tempfile::tempdir().unwrap();
        let include = temp.path().join("include");
        fs::create_dir_all(include.join("sodium")).unwrap();
        fs::write(include.join("sodium.h"), "header").unwrap();
        fs::write(include.join("sodium/version.h"), "version").unwrap();
        fs::write(include.join("sodium/export.h"), "exports").unwrap();
        let staged = stage_msvc_headers(&include, &temp.path().join("out"));
        let workaround = staged.join("../../../builds/msvc/version.h");
        assert_eq!(fs::read_to_string(&workaround).unwrap(), "version");
        fs::copy(workaround, staged.join("sodium/version.h")).unwrap();
        fs::write(staged.join("sodium/version.h"), "changed").unwrap();
        assert_eq!(
            fs::read_to_string(include.join("sodium/version.h")).unwrap(),
            "version"
        );
        assert_eq!(
            fs::read_to_string(staged.join("sodium.h")).unwrap(),
            "header"
        );
        assert_eq!(
            fs::read_to_string(include.join("sodium/export.h")).unwrap(),
            "exports"
        );
    }

    // Exercise MSVC declarations on Unix too; Windows socket tests check the final link.
    #[cfg(unix)]
    #[test]
    fn msvc_headers_declare_static_symbols() {
        let temp = tempfile::tempdir().unwrap();
        unpack_tar(
            local_archive(Path::new(VENDOR), "libsodium.tar.gz"),
            temp.path(),
        );
        let source = temp.path().join("libsodium-stable");
        let include = source.join("src/libsodium/include");
        fs::copy(
            source.join("builds/msvc/version.h"),
            include.join("sodium/version.h"),
        )
        .unwrap();
        let staged = stage_msvc_headers(&include, &temp.path().join("out"));
        let probe = temp.path().join("exports.c");
        fs::write(
            &probe,
            "#include <sodium/export.h>\n\
             #define STRINGIFY_INNER(x) #x\n\
             #define STRINGIFY(x) STRINGIFY_INNER(x)\n\
             _Static_assert(sizeof(STRINGIFY(SODIUM_EXPORT)) == 1, \"static export\");\n\
             _Static_assert(sizeof(STRINGIFY(SODIUM_EXPORT_WEAK)) == 1, \"static weak export\");\n",
        )
        .unwrap();
        let output = std::process::Command::new("cc")
            .args(["-std=c11", "-fsyntax-only", "-D_MSC_VER=1930", "-I"])
            .arg(staged)
            .arg(probe)
            .output()
            .expect("Cannot run the host C compiler");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
