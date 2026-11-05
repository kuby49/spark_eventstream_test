from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import types
import unittest
from unittest.mock import patch

from src.generator import producer as generator


class BufferedManifestTests(unittest.TestCase):
    def test_records_are_complete_and_fsynced_only_at_close(self):
        with TemporaryDirectory() as folder, patch.object(generator.os, "fsync") as sync:
            path = Path(folder) / "manifest"
            with generator.BufferedManifest(path) as manifest:
                for index in range(100):
                    manifest.write({"status": "generated", "index": index})
                self.assertEqual(sync.call_count, 0)
            self.assertEqual(sync.call_count, 1)
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([row["index"] for row in rows], list(range(100)))

    def test_final_fsync_failure_is_not_reported_as_success(self):
        with TemporaryDirectory() as folder, \
                patch.object(generator.os, "fsync", side_effect=OSError("disk")):
            with self.assertRaisesRegex(RuntimeError, "writer failed"):
                with generator.BufferedManifest(Path(folder) / "manifest") as manifest:
                    manifest.write({"status": "generated"})
            self.assertFalse(manifest.writer.is_alive())

    def test_600ms_storage_stall_does_not_block_sender_log_admission(self):
        entered, release = threading.Event(), threading.Event()
        with TemporaryDirectory() as folder:
            with generator.BufferedManifest(Path(folder) / "manifest") as manifest:
                original_write = manifest.file.write
                def slow_write(value):
                    entered.set()
                    if not release.wait(2):
                        raise TimeoutError("test release missing")
                    return original_write(value)
                with patch.object(manifest.file, "write", slow_write):
                    try:
                        manifest.write({"status": "generated", "index": 0})
                        self.assertTrue(entered.wait(2))
                        for index in range(1, 100):
                            manifest.write({"status": "generated", "index": index})
                    finally:
                        release.set()
            self.assertEqual(len((Path(folder) / "manifest").read_text().splitlines()), 100)

    def test_queue_is_bounded_and_reports_saturation(self):
        with TemporaryDirectory() as folder:
            manifest = generator.BufferedManifest(Path(folder) / "manifest")
            for index in range(4096):
                manifest.write({"status": "generated", "index": index})
            with self.assertRaisesRegex(RuntimeError, "queue saturated"):
                manifest.write({"status": "generated"})

    def test_delayed_schedule_continues_without_compressed_catchup(self):
        clock = [0.0]
        origin = datetime(2026, 11, 4, tzinfo=timezone.utc)
        calls, rows = [], []
        class ClockDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return origin + timedelta(seconds=clock[0])
        def submit(event):
            calls.append((clock[0], event))
            if len(calls) == 1:
                clock[0] += 0.6
        manifest = types.SimpleNamespace(
            buffered=True, last_timing=None, write=lambda row: rows.append(row))
        delivery = types.SimpleNamespace(check=lambda: None, submit=submit)
        with patch.object(generator, "datetime", ClockDateTime), \
                patch.object(generator.time, "monotonic", lambda: clock[0]), \
                patch.object(generator.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0]+seconds)), \
                patch.object(generator, "_write_manifest", lambda manifest, row: rows.append(row)):
            generator._send_schedule(390, 1, "run", manifest, delivery)
        self.assertEqual(len(calls), 390)
        self.assertEqual(sum(event["temperature"] > 80 for _, event in calls), 39)
        self.assertTrue(all(b[0]-a[0] >= 60/390-0.000001
                            for a, b in zip(calls, calls[1:])))
        self.assertFalse(any(row["status"] == "scheduling_failed" for row in rows))
        self.assertLess(60*389/(calls[-1][0]-calls[0][0]), 390)


if __name__ == "__main__":
    unittest.main()
