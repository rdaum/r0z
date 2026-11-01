#![cfg(all(unix, any(feature = "tokio", feature = "async-io", feature = "compio")))]

use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};

mod utils;

fn watchdog_report(scenario: &str) -> String {
    // Disable core files for this intentional abort. Leave libtest capture enabled in the child.
    let mut child = Command::new("sh")
        .args(["-c", "ulimit -c 0\nexec \"$@\"", "watchdog-test"])
        .arg(std::env::current_exe().unwrap())
        .args(["--exact", "watchdog_child"])
        .env("R0Z_WATCHDOG_CHILD", scenario)
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
    report.to_owned()
}

#[test]
fn watchdog_report_survives_abort_with_test_capture_enabled() {
    let report = watchdog_report("progress");
    assert!(report.contains("watchdog_child"), "{report}");
    assert!(report.contains("tests/watchdog.rs:"), "{report}");
    assert!(report.contains("adapter=watchdog-probe"), "{report}");
    assert!(report.contains("driver=probe-driver"), "{report}");
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
    let Ok(scenario) = std::env::var("R0Z_WATCHDOG_CHILD") else {
        return;
    };
    if scenario == "bind-failure" {
        let context = r0z::Context::new();
        let occupied = context.socket(r0z::REP).unwrap();
        occupied.bind("tcp://127.0.0.1:*").unwrap();
        let endpoint = occupied.get_last_endpoint().unwrap().unwrap();
        let deadline = utils::Deadline::start_with_timeout(Duration::from_millis(200));
        deadline.enter_adapter("bind-failure-probe");
        let echo = utils::sync_echo(endpoint, r0z::REP, 1);
        assert!(
            echo.join().is_err(),
            "binding an occupied endpoint succeeded"
        );
        utils::diagnostic_phase("awaiting reply from failed peer");
        loop {
            std::thread::park();
        }
    }
    assert_eq!(scenario, "progress");
    let deadline = utils::Deadline::start_with_timeout(Duration::from_millis(100));
    deadline.enter_adapter("watchdog-probe");
    utils::diagnostic_driver("probe-driver");
    let progress = utils::Progress::new("receiver", 1_000_000);
    progress.phase("receiving");
    utils::diagnostic_phase("awaiting message");
    // This count does not trigger periodic logging. The watchdog must read the live counter.
    progress.advance(42);
    loop {
        std::thread::park();
    }
}

#[test]
fn peer_bind_failure_survives_abort_with_test_capture_enabled() {
    let report = watchdog_report("bind-failure");
    assert!(report.contains("adapter=bind-failure-probe"), "{report}");
    assert!(
        report.contains("phase=awaiting reply from failed peer"),
        "{report}"
    );
    assert!(report.contains("echo=0/1 (binding)"), "{report}");
    assert!(report.contains("endpoint=tcp://127.0.0.1:"), "{report}");
    assert!(
        report.contains(&format!("error={}", r0z::Error::EADDRINUSE)),
        "{report}"
    );
}
