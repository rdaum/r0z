// Adapted from libsodium-sys-stable 1.24.3 by dnaq and jedisct1.
// Copyright (c) 2015 The sodiumoxide Developers
// Licensed under MIT OR Apache-2.0; see vendor/libsodium/BUILD-LICENSE-*.

use cc;
use std::{
    env,
    path::{Path, PathBuf},
};

pub fn compile(target: &str, source_dir: &Path, install_dir: &Path) -> Result<PathBuf, String> {
    use std::{fs, process::Command, str};

    // Decide on CC, CFLAGS and the --host configure argument
    let build_compiler = cc::Build::new().get_compiler();
    let mut compiler = build_compiler.cc_env().into_string().unwrap();
    if compiler.is_empty() {
        compiler = build_compiler.path().to_str().unwrap().to_owned();
    }
    let mut cflags = build_compiler.cflags_env().into_string().unwrap();
    let ldflags = env::var("SODIUM_LDFLAGS").unwrap_or_default();
    let host_arg;
    let help = if target.contains("-ios") {
        // Determine Xcode directory path
        let xcode_select_output = Command::new("xcode-select").arg("-p").output().unwrap();
        if !xcode_select_output.status.success() {
            return Err("Failed to run xcode-select -p".to_string());
        }
        let xcode_dir = str::from_utf8(&xcode_select_output.stdout)
            .unwrap()
            .trim()
            .to_string();

        // Determine SDK directory paths
        let sdk_dir_simulator = Path::new(&xcode_dir)
            .join("Platforms/iPhoneSimulator.platform/Developer/SDKs/iPhoneSimulator.sdk")
            .to_str()
            .unwrap()
            .to_string();
        let sdk_dir_ios = Path::new(&xcode_dir)
            .join("Platforms/iPhoneOS.platform/Developer/SDKs/iPhoneOS.sdk")
            .to_str()
            .unwrap()
            .to_string();

        // Min versions
        let ios_simulator_version_min = "9.0.0";
        let ios_version_min = "9.0.0";

        match target {
            "aarch64-apple-ios" => {
                cflags += " -arch arm64";
                cflags += &format!(" -isysroot {sdk_dir_ios}");
                cflags += &format!(" -mios-version-min={ios_version_min}");
                host_arg = "--host=aarch64-apple-darwin23".to_string();
            }
            "armv7-apple-ios" => {
                cflags += " -arch armv7";
                cflags += &format!(" -isysroot {sdk_dir_ios}");
                cflags += &format!(" -mios-version-min={ios_version_min}");
                cflags += " -mthumb";
                host_arg = "--host=arm-apple-darwin23".to_string();
            }
            "armv7s-apple-ios" => {
                cflags += " -arch armv7s";
                cflags += &format!(" -isysroot {sdk_dir_ios}");
                cflags += &format!(" -mios-version-min={ios_version_min}");
                cflags += " -mthumb";
                host_arg = "--host=arm-apple-darwin23".to_string();
            }
            "x86_64-apple-ios" => {
                cflags += " -arch x86_64";
                cflags += &format!(" -isysroot {sdk_dir_simulator}");
                cflags += &format!(" -mios-simulator-version-min={ios_simulator_version_min}");
                host_arg = "--host=x86_64-apple-darwin23".to_string();
            }
            "aarch64-apple-ios-sim" => {
                cflags += " -arch arm64";
                cflags += &format!(" -isysroot {sdk_dir_simulator}");
                cflags += &format!(" -mios-simulator-version-min={ios_simulator_version_min}");
                host_arg = "--host=aarch64-apple-darwin23".to_string();
            }
            _ => return Err(format!("Unknown iOS build target: {}", target)),
        }
        ""
    } else {
        if target.contains("i686") {
            compiler += " -m32 -maes";
            cflags += " -march=i686";
        }
        let host = env::var("HOST").unwrap();
        host_arg = format!("--host={target}");
        let cross_compiling = target != host;
        if cross_compiling {
            "***********************************************************\n\
             Use the 'cargo zigbuild' command to cross-compile Rust code\n\
             with C dependencies such as libsodium.\n\
             ***********************************************************\n"
        } else {
            ""
        }
    };

    // Run `./configure`
    let prefix_arg = format!("--prefix={}", install_dir.to_str().unwrap());
    let mut configure_cmd = Command::new(fs::canonicalize(source_dir.join("configure")).unwrap());
    if !compiler.is_empty() {
        configure_cmd.env("CC", &compiler);
    }
    if !cflags.is_empty() {
        configure_cmd.env("CFLAGS", &cflags);
    }
    if !ldflags.is_empty() {
        configure_cmd.env("LDFLAGS", &ldflags);
    }
    if env::var("SODIUM_DISABLE_PIE").is_ok() {
        configure_cmd.arg("--disable-pie");
    }
    configure_cmd.env("AR", cc::Build::new().get_archiver().get_program());
    configure_cmd.env("RANLIB", cc::Build::new().get_ranlib().get_program());
    configure_cmd.arg("--disable-ssp");
    let configure_output = configure_cmd
        .current_dir(source_dir)
        .arg(&prefix_arg)
        .arg(&host_arg)
        .arg("--enable-shared=no")
        .arg("--disable-dependency-tracking")
        .output();
    let configure_output = match configure_output {
        Ok(output) => output,
        Err(error) => {
            return Err(format!("Failed to run './configure': {}\n{}", error, help));
        }
    };
    if !configure_output.status.success() {
        return Err(format!(
            "\n{:?}\nCFLAGS={}\nLDFLAGS={}\nCC={}\n{}\n{}\n{}\n",
            configure_cmd,
            cflags,
            ldflags,
            compiler,
            String::from_utf8_lossy(&configure_output.stdout),
            String::from_utf8_lossy(&configure_output.stderr),
            help
        ));
    }

    let j_arg = format!("-j{}", env::var("NUM_JOBS").unwrap());

    // Run `make install`
    let mut install_cmd = Command::new("make");
    let install_output = install_cmd
        .current_dir(source_dir)
        .arg(j_arg)
        .arg("install")
        .output();
    let install_output = match install_output {
        Ok(install_output) => install_output,
        Err(error) => {
            return Err(format!("Failed to run 'make install': {}\n", error));
        }
    };
    if !install_output.status.success() {
        panic!(
            "\n{}\n{}\n{}\n",
            String::from_utf8_lossy(&configure_output.stdout),
            String::from_utf8_lossy(&install_output.stdout),
            String::from_utf8_lossy(&install_output.stderr)
        );
    }
    Ok(install_dir.join("lib"))
}
