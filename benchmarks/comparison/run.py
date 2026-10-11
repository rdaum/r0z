#!/usr/bin/env python3
"""Build and run isolated TCP comparison peers. Requires Linux and Python 3.11+."""

import argparse
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import platform
import random
import signal
import socket
import subprocess
import sys
import time
import tomllib
import uuid

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
TARGET = ROOT / "target" / "comparison"
IMPLEMENTATIONS = {
    "r0z": ("r0z", 1),
    "tmq": ("tmq", 1),
    "omq-ct": ("omq", 0),
    "omq-io1": ("omq", 1),
}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def capture(*command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def build(backends, jobs, run_dir):
    binaries = {}
    records = {}
    env = os.environ.copy()
    # Use one explicit Rust code-generation policy for all standalone projects.
    env.pop("CARGO_ENCODED_RUSTFLAGS", None)
    env["RUSTFLAGS"] = ""
    env["CARGO_PROFILE_RELEASE_OPT_LEVEL"] = "3"
    env["CARGO_PROFILE_RELEASE_CODEGEN_UNITS"] = "16"
    env["CARGO_PROFILE_RELEASE_LTO"] = "false"
    env["CARGO_PROFILE_RELEASE_DEBUG"] = "false"
    for backend in sorted(backends):
        manifest = HERE / backend / "Cargo.toml"
        lock = manifest.with_name("Cargo.lock")
        if not lock.exists():
            raise RuntimeError(f"missing lockfile: {lock}")
        command = [
            "cargo",
            "build",
            "--release",
            "--locked",
            "--jobs",
            str(jobs),
            "--manifest-path",
            str(manifest),
            "--target-dir",
            str(TARGET / "build" / backend),
            "--message-format=json-render-diagnostics",
        ]
        print(f"Building {backend}", flush=True)
        with (run_dir / f"build-{backend}.jsonl").open("w") as output:
            subprocess.run(command, cwd=ROOT, env=env, stdout=output, check=True)
        artifacts = [
            json.loads(line)
            for line in (run_dir / f"build-{backend}.jsonl").read_text().splitlines()
        ]
        executable = next(
            row["executable"]
            for row in reversed(artifacts)
            if row.get("reason") == "compiler-artifact"
            and row.get("executable")
            and row["target"]["name"] == f"comparison-{backend}"
        )
        binaries[backend] = executable
        (run_dir / f"Cargo-{backend}.lock").write_bytes(lock.read_bytes())
        records[backend] = {
            "command": command,
            "rustflags": env["RUSTFLAGS"],
            "profile": {
                key: value
                for key, value in env.items()
                if key.startswith("CARGO_PROFILE_RELEASE_")
            },
            "binary": executable,
            "binary_sha256": sha256(Path(executable)),
            "lock_sha256": sha256(lock),
            "packages": tomllib.loads(lock.read_text())["package"],
        }
    return binaries, records


def cpu_set(value):
    if value is None:
        return None
    cpus = {int(part) for part in value.split(",")}
    if not cpus or not cpus <= os.sched_getaffinity(0):
        raise ValueError(f"CPUs must belong to the current affinity mask: {value}")
    return cpus


class Peer:
    def __init__(self, binary, config, cpus, log_path, deadline):
        self.deadline = deadline
        self.process = None
        self.connection = None
        self.reader = None
        self.log = log_path.open("wb")
        try:
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                listener.listen(1)
                listener.settimeout(self.remaining())
                config = dict(config, control=f"127.0.0.1:{listener.getsockname()[1]}")
                # The runner is single-threaded. Apply affinity before exec so all
                # runtime and native threads inherit the same process mask.
                self.process = subprocess.Popen(
                    [binary, json.dumps(config)],
                    stdout=self.log,
                    stderr=self.log,
                    start_new_session=True,
                    env={
                        key: value
                        for key, value in os.environ.items()
                        if not key.startswith(("OMQ_", "ZMQ_"))
                    },
                    preexec_fn=(lambda: os.sched_setaffinity(0, cpus))
                    if cpus
                    else None,
                )
                self.connection, _ = listener.accept()
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.reader = self.connection.makefile("rb")
            self.ready = self.receive("ready")
        except BaseException:
            self.close()
            raise

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("trial deadline exceeded")
        return remaining

    def send(self, command):
        self.connection.settimeout(self.remaining())
        self.connection.sendall((command + "\n").encode())

    def receive(self, expected):
        self.connection.settimeout(self.remaining())
        line = self.reader.readline()
        if not line:
            raise RuntimeError(f"peer exited before {expected}; see peer log")
        row = json.loads(line)
        if row.get("event") != expected:
            raise RuntimeError(f"expected {expected}, received {row}")
        return row

    def placement(self):
        tasks = Path(f"/proc/{self.process.pid}/task")
        return {
            "affinity": sorted(os.sched_getaffinity(self.process.pid)),
            "threads": len(list(tasks.iterdir())),
        }

    def finish(self):
        code = self.process.wait(timeout=self.remaining())
        if code != 0:
            raise RuntimeError(f"peer exited with status {code}; see peer log")

    def close(self):
        if self.process is not None and self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process.wait()
        if self.reader is not None:
            self.reader.close()
        if self.connection is not None:
            self.connection.close()
        self.log.close()


def delivery_metrics(pattern, count, size, sender, receiver):
    for measurement in (sender, receiver):
        if measurement["count"] != count:
            raise ValueError("delivery count mismatch")
        if measurement["end_ns"] <= measurement["start_ns"]:
            raise ValueError("invalid measurement interval")
        elapsed = (measurement["end_ns"] - measurement["start_ns"]) / 1e9
        cpu = measurement["cpu_seconds"]
        if not math.isfinite(cpu) or cpu < 0:
            raise ValueError("invalid CPU delta")
        if not math.isclose(measurement["elapsed_seconds"], elapsed, rel_tol=1e-9):
            raise ValueError("wall interval mismatch")
        if not math.isclose(
            measurement["cpu_percent"], 100 * cpu / elapsed, rel_tol=1e-9, abs_tol=1e-9
        ):
            raise ValueError("CPU percentage uses a different interval")
    end_ns = receiver["end_ns"] if pattern == "pushpull" else sender["end_ns"]
    duration = (end_ns - sender["start_ns"]) / 1e9
    if duration <= 0 or receiver["start_ns"] > sender["start_ns"]:
        raise ValueError("receiver was not armed before sender")
    if sender["end_ns"] < end_ns:
        raise ValueError("sender stopped before delivery")
    if pattern == "reqrep" and receiver["end_ns"] < sender["end_ns"]:
        raise ValueError("responder stopped before final echo delivery")
    rtts = sorted(sender["rtt_ns"])
    if len(rtts) != (count if pattern == "reqrep" else 0) or receiver["rtt_ns"]:
        raise ValueError("invalid RTT sample count")
    if any(value < 0 for value in rtts):
        raise ValueError("invalid RTT duration")
    return {
        "delivery_seconds": duration,
        "messages_per_second": count / duration,
        "payload_bytes_per_second": count * size / duration,
        "p50_ns": rtts[(len(rtts) - 1) // 2] if rtts else None,
        "p99_ns": rtts[(99 * len(rtts) + 99) // 100 - 1] if len(rtts) >= 1000 else None,
    }


def trial(binary, case, args, run_dir, trial_id):
    backend, io_threads = IMPLEMENTATIONS[case["implementation"]]
    requested = args.messages if case["pattern"] == "pushpull" else args.iterations
    count = min(requested, args.max_payload_bytes // case["size"])
    warmup = min(args.warmup, args.max_payload_bytes // case["size"])
    config = {
        "pattern": case["pattern"],
        "payload_mode": case["payload_mode"],
        "size": case["size"],
        "count": count,
        "warmup": warmup,
        "hwm": args.hwm,
        "io_threads": io_threads,
        "verify_full": args.verify_full,
    }
    receiver = sender = None
    deadline = time.monotonic() + args.timeout
    try:
        receiver = Peer(
            binary[backend],
            dict(config, role="receiver", endpoint=""),
            args.receiver_cpus,
            run_dir / f"{trial_id}-receiver.log",
            deadline,
        )
        sender = Peer(
            binary[backend],
            dict(config, role="sender", endpoint=receiver.ready["endpoint"]),
            args.sender_cpus,
            run_dir / f"{trial_id}-sender.log",
            deadline,
        )
        receiver.send("warmup")
        sender.send("warmup")
        receiver.receive("warm")
        sender.receive("warm")
        # Warmup finishes at both peers before payload preparation or measurement.
        receiver.send("prepare")
        sender.send("prepare")
        receiver.receive("prepared")
        sender.receive("prepared")
        placement = {"sender": sender.placement(), "receiver": receiver.placement()}
        receiver.send("run")
        receiver.receive("armed")
        sender.send("run")
        if case["pattern"] == "pushpull":
            received = receiver.receive("result")["measurement"]
            sender.send("delivered")
            sent = sender.receive("result")["measurement"]
        else:
            sent = sender.receive("result")["measurement"]
            receiver.send("delivered")
            received = receiver.receive("result")["measurement"]
        metrics = delivery_metrics(case["pattern"], count, case["size"], sent, received)
        sender.send("quit")
        receiver.send("quit")
        sender.finish()
        receiver.finish()
        return {
            "config": config,
            "sender": sent,
            "receiver": received,
            "metrics": metrics,
            "placement": placement,
            "ready": {"sender": sender.ready, "receiver": receiver.ready},
        }
    finally:
        for peer in (sender, receiver):
            if peer is not None:
                peer.close()


def schedule(implementations, patterns, modes, sizes, rounds, seed):
    rng = random.Random(seed)
    for round_index in range(rounds):
        cases = list(itertools.product(patterns, modes, sizes))
        rng.shuffle(cases)
        for pattern, mode, size in cases:
            order = list(implementations)
            rng.shuffle(order)
            for implementation in order:
                yield {
                    "round": round_index,
                    "implementation": implementation,
                    "pattern": pattern,
                    "payload_mode": mode,
                    "size": size,
                }


def source_hashes():
    paths = (
        list(HERE.rglob("*.rs"))
        + list(HERE.rglob("*.py"))
        + list(HERE.rglob("Cargo.toml"))
    )
    for crate in (ROOT / "crates").iterdir():
        paths.extend(crate.rglob("*.rs"))
        paths.append(crate / "Cargo.toml")
    paths.append(ROOT / "Cargo.toml")
    return {
        str(path.relative_to(ROOT)): sha256(path)
        for path in sorted(set(paths))
        if "target" not in path.relative_to(ROOT).parts
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--implementations",
        nargs="+",
        choices=IMPLEMENTATIONS,
        default=list(IMPLEMENTATIONS),
    )
    parser.add_argument(
        "--patterns",
        nargs="+",
        choices=["pushpull", "reqrep"],
        default=["pushpull", "reqrep"],
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=["fresh", "prepared"],
        default=["fresh", "prepared"],
    )
    parser.add_argument("--sizes", type=int, nargs="+", default=[16, 4096, 1048576])
    parser.add_argument("--messages", type=int, default=10000)
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=128)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--hwm", type=int, default=1000)
    parser.add_argument("--max-payload-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--sender-cpus")
    parser.add_argument("--receiver-cpus")
    parser.add_argument(
        "--verify-full",
        action="store_true",
        help="scan every payload byte during measurement",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="one short validation round, with full payload checks",
    )
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("this harness currently requires Linux")
    if args.smoke:
        args.messages = min(args.messages, 32)
        args.iterations = min(args.iterations, 16)
        args.warmup = min(args.warmup, 4)
        args.rounds = 1
        args.verify_full = True
    if any(
        value < 1
        for value in [
            args.messages,
            args.iterations,
            args.warmup,
            args.rounds,
            args.hwm,
            args.jobs,
        ]
    ):
        parser.error("counts, HWM, and jobs must be positive")
    if args.hwm > 2**31 - 1 or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("invalid HWM or timeout")
    if any(size < 8 or size > args.max_payload_bytes for size in args.sizes):
        parser.error("sizes must be between 8 and --max-payload-bytes")
    args.sender_cpus = cpu_set(args.sender_cpus)
    args.receiver_cpus = cpu_set(args.receiver_cpus)
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:8]
    run_dir = TARGET / "runs" / run_id
    run_dir.mkdir(parents=True)
    print(f"Run: {run_dir}", flush=True)
    cases = list(
        schedule(
            args.implementations,
            args.patterns,
            args.modes,
            args.sizes,
            args.rounds,
            args.seed,
        )
    )
    metadata = {
        "schema": 1,
        "run_id": run_id,
        "validation_only": args.smoke,
        "arguments": vars(args),
        "schedule": cases,
        "source_hashes": source_hashes(),
        "git_head": capture("git", "rev-parse", "HEAD"),
        "git_status": capture("git", "status", "--short"),
        "rustc": capture("rustc", "-vV"),
        "cargo": capture("cargo", "-V"),
        "host": platform.uname()._asdict(),
        "cpu_info": Path("/proc/cpuinfo").read_text(),
        "affinity": sorted(os.sched_getaffinity(0)),
        "load_before": os.getloadavg(),
        "build_environment": {
            key: value
            for key, value in os.environ.items()
            if key in ["CC", "CXX", "CFLAGS", "CXXFLAGS", "LDFLAGS", "RUSTC_WRAPPER"]
        },
    }
    for relative in metadata["source_hashes"]:
        destination = run_dir / "sources" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((ROOT / relative).read_bytes())

    def save_metadata():
        (run_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2, default=lambda x: sorted(x)) + "\n"
        )

    save_metadata()
    try:
        binaries, metadata["builds"] = build(
            {IMPLEMENTATIONS[name][0] for name in args.implementations},
            args.jobs,
            run_dir,
        )
        if source_hashes() != metadata["source_hashes"]:
            raise RuntimeError("sources changed during the build; discard this run")
        save_metadata()
        with (run_dir / "samples.jsonl").open("w", buffering=1) as samples:
            for index, case in enumerate(cases):
                trial_id = f"{index:04d}"
                row = {
                    "run_id": run_id,
                    "trial_id": trial_id,
                    **case,
                    "load_before": os.getloadavg(),
                }
                try:
                    row.update(trial(binaries, case, args, run_dir, trial_id))
                    row["status"] = "ok"
                except Exception as error:
                    row.update(status="failed", error=repr(error))
                    samples.write(json.dumps(row) + "\n")
                    raise
                row["load_after"] = os.getloadavg()
                samples.write(json.dumps(row) + "\n")
                print(
                    f"[{index + 1}/{len(cases)}] {case['implementation']} {case['pattern']} "
                    f"{case['payload_mode']} {case['size']} bytes: validated",
                    flush=True,
                )
        if source_hashes() != metadata["source_hashes"]:
            raise RuntimeError("sources changed during measurement; discard this run")
        metadata["status"] = "complete"
    except BaseException as error:
        metadata.update(status="failed", error=repr(error))
        raise
    finally:
        metadata["load_after"] = os.getloadavg()
        save_metadata()
    print(f"Raw results: {run_dir / 'samples.jsonl'}")


if __name__ == "__main__":
    main()
