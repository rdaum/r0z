import json
from pathlib import Path
import tempfile
import time
import unittest

import run


def measurement(start, end, count=3, cpu=0.5, rtts=None):
    elapsed = (end - start) / 1e9
    return {
        "start_ns": start,
        "end_ns": end,
        "count": count,
        "cpu_seconds": cpu,
        "elapsed_seconds": elapsed,
        "cpu_percent": 100 * cpu / elapsed,
        "rtt_ns": rtts or [],
    }


class MeasurementTests(unittest.TestCase):
    def test_throughput_ends_at_receipt_not_queueing_or_control_ack(self):
        sender = measurement(2_000_000_000, 6_000_000_000)
        receiver = measurement(1_000_000_000, 5_000_000_000)
        result = run.delivery_metrics("pushpull", 3, 16, sender, receiver)
        self.assertEqual(result["delivery_seconds"], 3)
        self.assertEqual(result["messages_per_second"], 1)
        self.assertIsNone(result["p99_ns"])

    def test_incomplete_delivery_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "count mismatch"):
            run.delivery_metrics(
                "pushpull", 3, 16, measurement(2, 4), measurement(1, 3, count=2)
            )

    def test_cpu_percentage_must_use_its_own_window(self):
        sender = measurement(2_000_000_000, 6_000_000_000)
        sender["cpu_percent"] = 100 * sender["cpu_seconds"] / 3
        with self.assertRaisesRegex(ValueError, "different interval"):
            run.delivery_metrics(
                "pushpull", 3, 16, sender, measurement(1_000_000_000, 5_000_000_000)
            )

    def test_latency_requires_every_reply_sample(self):
        with self.assertRaisesRegex(ValueError, "RTT sample count"):
            run.delivery_metrics(
                "reqrep", 3, 16, measurement(2, 4, rtts=[1]), measurement(1, 5)
            )

    def test_seeded_schedule_covers_all_cases_each_round(self):
        arguments = (
            ["r0z", "tmq", "omq-ct"],
            ["pushpull", "reqrep"],
            ["fresh", "prepared"],
            [16, 4096],
            3,
            42,
        )
        rows = list(run.schedule(*arguments))
        self.assertEqual(rows, list(run.schedule(*arguments)))
        for round_index in range(3):
            cases = [
                json.dumps(row, sort_keys=True)
                for row in rows
                if row["round"] == round_index
            ]
            self.assertEqual(len(cases), 24)
            self.assertEqual(len(set(cases)), 24)

    def test_stalled_peer_is_killed_by_external_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            peer = path / "stalled.py"
            peer.write_text(
                "#!/usr/bin/env python3\nimport json, socket, sys, time\n"
                "config = json.loads(sys.argv[1])\n"
                "host, port = config['control'].split(':')\n"
                "connection = socket.create_connection((host, int(port)))\n"
                "time.sleep(60)\n"
            )
            peer.chmod(0o755)
            before = time.monotonic()
            with self.assertRaises(TimeoutError):
                run.Peer(str(peer), {}, None, path / "peer.log", before + 0.3)
            self.assertLess(time.monotonic() - before, 3)


if __name__ == "__main__":
    unittest.main()
