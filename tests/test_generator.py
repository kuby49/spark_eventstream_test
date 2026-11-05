import random
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import types
from datetime import datetime, timedelta, timezone
import unittest
from uuid import uuid4
from unittest.mock import patch

from src.generator.producer import build_minute, generated_manifest_record, validate_rate
from src.generator import producer as generator


class GeneratorTests(unittest.TestCase):
    def setUp(self):
        class ScheduleDelivery:
            maximum_inflight = 64
            def __init__(self, producer, topic, manifest, run_id):
                self.producer, self.topic, self.manifest = producer, topic, manifest
            def start(self):
                pass
            def check(self):
                pass
            def submit(self, event):
                generator.publish_event(self.producer, self.topic, event, self.manifest)
            def finish(self):
                return 0
        self.tracker = patch.object(generator, "BackgroundDelivery", ScheduleDelivery)
        self.tracker.start()
        self.addCleanup(self.tracker.stop)

    def test_delivery_timing_keeps_generated_and_acknowledged_records_durable(self):
        order = []
        class Producer:
            def produce(self, topic, key, value, on_delivery):
                order.append("produce")
                on_delivery(None, object())
            def flush(self, timeout):
                order.append("delivery_flush")
        with TemporaryDirectory() as folder, patch.object(
                generator.os, "fsync", lambda descriptor: order.append("fsync")):
            path = Path(folder) / "manifest.jsonl"
            event = build_minute(330, "run", random.Random(7))[0]
            with generator.DurableManifest(path) as manifest:
                generator._write_manifest(manifest, generated_manifest_record(event))
                self.assertTrue(generator.publish_event(Producer(), "topic", event, manifest))
            self.assertEqual(order, ["fsync", "produce", "delivery_flush", "fsync"])
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(rows[1]["previous_manifest_timing"]["status"], "generated")
            self.assertGreaterEqual(rows[1]["send_to_ack_seconds"], 0)
            self.assertEqual(rows[1]["attempts"], 1)

    def test_persistent_manifest_fsyncs_each_record_and_propagates_storage_failure(self):
        with TemporaryDirectory() as folder:
            path = Path(folder) / "manifest.jsonl"
            with generator.DurableManifest(path) as manifest:
                with patch.object(generator.os, "fsync") as sync:
                    generator._write_manifest(manifest, {"status": "generated"})
                    handle = manifest.file
                    generator._write_manifest(manifest, {"status": "acknowledged"})
                    self.assertIs(manifest.file, handle)
                    self.assertEqual(sync.call_count, 2)
                    self.assertEqual(manifest.last_timing["status"], "acknowledged")
                with patch.object(generator.os, "fsync", side_effect=OSError("disk failure")):
                    with self.assertRaisesRegex(OSError, "disk failure"):
                        generator._write_manifest(manifest, {"status": "generated"})
            self.assertTrue(handle.closed)

    def test_slow_fsync_is_measured_and_pacing_guard_is_not_relaxed(self):
        start = datetime(2026, 11, 3, tzinfo=timezone.utc)
        clock = [0.0]
        class ClockDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return start + timedelta(seconds=clock[0])
        def sync(descriptor):
            clock[0] += 0.6
        kafka = types.SimpleNamespace(Producer=lambda config: types.SimpleNamespace(
            list_topics=lambda **kwargs: types.SimpleNamespace(
                topics={"topic": types.SimpleNamespace(error=None)}),
            flush=lambda timeout: None))
        with TemporaryDirectory() as folder, \
                patch.dict(sys.modules, {"confluent_kafka": kafka}), \
                patch.object(generator, "datetime", ClockDateTime), \
                patch.object(generator.time, "monotonic", lambda: clock[0]), \
                patch.object(generator.os, "fsync", sync), \
                patch.object(generator, "_load_source_connection",
                             return_value=("private:9093", "topic", "Endpoint=sb://private;EntityPath=topic")), \
                patch.object(generator, "publish_event", return_value=True):
            path = Path(folder) / "manifest.jsonl"
            with self.assertRaisesRegex(RuntimeError, "Producer exceeded one event interval"):
                generator.run(330, 1, "run", path)
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([row["status"] for row in rows], ["generated", "scheduling_failed"])
            self.assertAlmostEqual(rows[1]["previous_manifest_timing"]["fsync_seconds"], 0.6)
            self.assertEqual(rows[1]["scheduled_index"], 1)

    def test_metadata_warmup_precedes_schedule_and_slow_delivery_still_stops(self):
        start = datetime(2026, 11, 3, tzinfo=timezone.utc)
        clock = [0.0]
        class ClockDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return start + timedelta(seconds=clock[0])
        def metadata(**kwargs):
            clock[0] += 3
            return types.SimpleNamespace(topics={"topic": types.SimpleNamespace(error=None)})
        def publish(producer, topic, event, manifest):
            self.assertEqual(clock[0], 3)
            clock[0] += 0.6
            return True
        kafka = types.SimpleNamespace(Producer=lambda config: types.SimpleNamespace(
            list_topics=metadata, flush=lambda timeout: None))
        with TemporaryDirectory() as folder, \
                patch.dict(sys.modules, {"confluent_kafka": kafka}), \
                patch.object(generator, "datetime", ClockDateTime), \
                patch.object(generator.time, "monotonic", lambda: clock[0]), \
                patch.object(generator.time, "sleep", lambda seconds: None), \
                patch.object(generator, "_load_source_connection",
                             return_value=("private:9093", "topic", "Endpoint=sb://private;EntityPath=topic")), \
                patch.object(generator, "publish_event", publish):
            path = Path(folder) / "manifest.jsonl"
            with self.assertRaisesRegex(RuntimeError, "Producer exceeded one event interval"):
                generator.run(240, 1, "run", path)
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([row["status"] for row in rows], ["generated", "scheduling_failed"])
            self.assertEqual(rows[0]["scheduled_at"], "2026-11-03T00:00:03Z")
            self.assertEqual(rows[0]["scheduling_lag_seconds"], 0)
            self.assertEqual(rows[1]["scheduled_index"], 1)
            self.assertAlmostEqual(rows[1]["monotonic_lag_seconds"], 0.35)
            self.assertEqual(rows[1]["interval_seconds"], 0.25)

    def test_utc_clock_slew_does_not_send_before_scheduled_time(self):
        start = datetime(2026, 11, 3, tzinfo=timezone.utc)
        clock = [0.0]
        class ClockDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return start + timedelta(seconds=clock[0] - (0.025 if clock[0] >= 2 else 0))
        def sleep(seconds):
            clock[0] += seconds
        kafka = types.SimpleNamespace(Producer=lambda config: types.SimpleNamespace(
            flush=lambda timeout: None, list_topics=lambda **kwargs: types.SimpleNamespace(
                topics={"topic": types.SimpleNamespace(error=None)})))
        with TemporaryDirectory() as folder, \
                patch.dict(sys.modules, {"confluent_kafka": kafka}), \
                patch.object(generator, "datetime", ClockDateTime), \
                patch.object(generator.time, "monotonic", lambda: clock[0]), \
                patch.object(generator.time, "sleep", sleep), \
                patch.object(generator, "_load_source_connection",
                             return_value=("private:9093", "topic", "Endpoint=sb://private;EntityPath=topic")), \
                patch.object(generator, "publish_event", return_value=True):
            path = Path(folder) / "manifest.jsonl"
            self.assertEqual(generator.run(30, 1, "run", path), 0)
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            for row in rows:
                if row["status"] != "generated":
                    continue
                actual = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
                scheduled = datetime.fromisoformat(row["scheduled_at"].replace("Z", "+00:00"))
                self.assertGreaterEqual(actual, scheduled)
                self.assertLessEqual((actual - scheduled).total_seconds(), 2)

    def test_run_timestamps_immediately_before_send_and_records_schedule_separately(self):
        start = datetime(2026, 11, 3, tzinfo=timezone.utc)
        clock = [0.0]
        class ClockDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return start + timedelta(seconds=clock[0])
        def sleep(seconds):
            clock[0] += seconds
        def publish(producer, topic, event, manifest):
            self.assertEqual(datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00")),
                             ClockDateTime.now())
            self.assertEqual(event["payload"]["timestamp"], event["timestamp"])
            return True
        kafka = types.SimpleNamespace(Producer=lambda config: types.SimpleNamespace(
            flush=lambda timeout: None, list_topics=lambda **kwargs: types.SimpleNamespace(
                topics={"topic": types.SimpleNamespace(error=None)})))
        with TemporaryDirectory() as folder, \
                patch.dict(sys.modules, {"confluent_kafka": kafka}), \
                patch.object(generator, "datetime", ClockDateTime), \
                patch.object(generator.time, "monotonic", lambda: clock[0]), \
                patch.object(generator.time, "sleep", sleep), \
                patch.object(generator, "_load_source_connection",
                             return_value=("private:9093", "topic", "Endpoint=sb://private;EntityPath=topic")), \
                patch.object(generator, "publish_event", publish):
            path = Path(folder) / "manifest.jsonl"
            self.assertEqual(generator.run(30, 1, "run", path), 0)
            rows = [json.loads(line) for line in path.read_text().splitlines()
                    if json.loads(line)["status"] == "generated"]
            self.assertEqual(len(rows), 30)
            self.assertEqual({row["scheduled_minute"] for row in rows}, {0})
            self.assertEqual([row["scheduled_index"] for row in rows], list(range(30)))
            self.assertEqual(sum(row["temperature"] > 80 for row in rows), 3)

    def test_scheduled_minutes_have_single_anchor_without_cumulative_drift(self):
        start = datetime(2026, 11, 3, tzinfo=timezone.utc)
        first = build_minute(30, "run", random.Random(1), minute_start=start)
        second = build_minute(30, "run", random.Random(2), minute_start=start + timedelta(minutes=1))
        self.assertEqual(datetime.fromisoformat(second[0]["timestamp"].replace("Z", "+00:00"))
                         - datetime.fromisoformat(first[-1]["timestamp"].replace("Z", "+00:00")),
                         timedelta(seconds=2))

    def test_default_rate_has_exact_high_fraction_and_valid_ranges(self):
        events = build_minute(30, str(uuid4()), random.Random(7))
        self.assertEqual(len(events), 30)
        self.assertEqual(sum(event["temperature"] > 80 for event in events), 3)
        self.assertTrue(all(0 <= event["equipment_id"] <= 100 for event in events))
        self.assertTrue(all(0 <= event["temperature"] <= 100 for event in events))
        self.assertEqual(len({event["message_id"] for event in events}), 30)
        timestamps = [event["timestamp"] for event in events]
        self.assertEqual(timestamps, sorted(timestamps))
        self.assertTrue(
            all(
                event["payload"]
                == {
                    key: value
                    for key, value in event.items()
                    if key != "payload"
                }
                for event in events
            )
        )

    def test_rate_must_preserve_exact_ten_percent_rule(self):
        for rate in (0, 1, 29, 31):
            with self.subTest(rate=rate), self.assertRaises(ValueError):
                validate_rate(rate)

    def test_other_rates_divisible_by_ten_are_supported(self):
        events = build_minute(40, str(uuid4()), random.Random(11))
        self.assertEqual(sum(event["temperature"] > 80 for event in events), 4)

    def test_generated_manifest_preserves_payload_for_reconciliation(self):
        event = build_minute(30, str(uuid4()), random.Random(13))[0]
        record = generated_manifest_record(event)
        self.assertEqual(record["status"], "generated")
        self.assertEqual(record["message_id"], event["message_id"])
        self.assertEqual(record["run_id"], event["run_id"])
        self.assertEqual(record["equipment_id"], event["equipment_id"])
        self.assertEqual(record["temperature"], event["temperature"])
        self.assertEqual(record["timestamp"], event["timestamp"])
        self.assertEqual(record["payload"], event)


if __name__ == "__main__":
    unittest.main()
