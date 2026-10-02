#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};

mod utils;

#[test]
fn watchdog_report_survives_abort_with_test_capture_enabled() {
    // Disable core files for this intentional abort. Leave libtest capture enabled in the child.
    let mut child = Command::new("sh")
        .args(["-c", "ulimit -c 0\nexec \"$@\"", "watchdog-test"])
        .arg(std::env::current_exe().unwrap())
        .args(["--exact", "watchdog_child"])
        .env("R0Z_WATCHDOG_CHILD", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child.wait_with_output().unwrap();
            panic!(
                "watchdog child exceeded process deadline: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "watchdog did not abort");
    let report = stderr
        .lines()
        .find(|line| line.contains("deadline exceeded"))
        .unwrap_or_else(|| panic!("missing watchdog report after abort: {stderr}"));
    assert!(report.contains("watchdog_child"), "{report}");
    assert!(report.contains("tests/watchdog.rs:"), "{report}");
    assert!(report.contains("adapter=watchdog-probe"), "{report}");
    assert!(report.contains("phase=awaiting message"), "{report}");
    assert!(
        report.contains("receiver=42/1000000 (receiving)"),
        "{report}"
    );
    assert!(
        report.contains("deadline exceeded (0.100s); aborting"),
        "{report}"
    );
}

#[test]
fn watchdog_child() {
    if std::env::var_os("R0Z_WATCHDOG_CHILD").is_none() {
        return;
    }
    let deadline = utils::Deadline::start_with_timeout(Duration::from_millis(100));
    deadline.enter_adapter("watchdog-probe");
    let progress = utils::Progress::new("receiver", 1_000_000);
    progress.phase("receiving");
    utils::diagnostic_phase("awaiting message");
    // This count does not trigger periodic logging. The watchdog must read the live counter.
    progress.advance(42);
    loop {
        std::thread::park();
    }
}
