"""Historical serial-delivery contract probes used for comparison."""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
import sys
from tempfile import TemporaryDirectory
import types
import unittest
from unittest.mock import patch

from src.generator import producer as generator


class ProducerCapabilityTests(unittest.TestCase):
    def run_with_clock(self, folder, delivery_seconds=0, sync_seconds=0,
                       sync_failure=None, stop_after_publish=False):
        clock = [0.0]
        published = []
        sync_count = [0]
        origin = datetime(2026, 11, 4, tzinfo=timezone.utc)

        class ClockDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return origin + timedelta(seconds=clock[0])

        class Producer:
            callback = None

            def list_topics(self, **kwargs):
                return types.SimpleNamespace(
                    topics={"topic": types.SimpleNamespace(error=None)})

            def produce(self, topic, key, value, on_delivery):
                published.append((clock[0], json.loads(value)))
                if stop_after_publish:
                    raise KeyboardInterrupt("simulated process loss after enqueue")
                self.callback = on_delivery

            def flush(self, timeout):
                if self.callback:
                    clock[0] += delivery_seconds
                    callback, self.callback = self.callback, None
                    callback(None, object())

        def sync(descriptor):
            sync_count[0] += 1
            clock[0] += sync_seconds
            if sync_count[0] == sync_failure:
                raise OSError("simulated durable writer failure")

        def sleep(seconds):
            clock[0] += seconds

        path = Path(folder) / "manifest.jsonl"
        kafka = types.SimpleNamespace(Producer=lambda config: Producer())
        class SerialReference:
            maximum_inflight = 1
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
        with patch.dict(sys.modules, {"confluent_kafka": kafka}), \
                patch.object(generator, "BackgroundDelivery", SerialReference), \
                patch.object(generator, "datetime", ClockDateTime), \
                patch.object(generator.time, "monotonic", lambda: clock[0]), \
                patch.object(generator.time, "sleep", sleep), \
                patch.object(generator.os, "fsync", sync), \
                patch.object(generator, "_load_source_connection", return_value=(
                    "private:9093", "topic", "Endpoint=sb://private;EntityPath=topic")):
            try:
                generator.run(360, 1, "same-run", path)
            except BaseException as error:
                return published, error
        return published, None

    def test_600ms_delivery_cannot_meet_interval_under_ack_before_next_send(self):
        with TemporaryDirectory() as folder:
            published, error = self.run_with_clock(folder, delivery_seconds=0.6)
            self.assertIsInstance(error, RuntimeError)
            self.assertEqual(len(published), 1)
            rows = [json.loads(line) for line in
                    (Path(folder) / "manifest.jsonl").read_text().splitlines()]
            self.assertEqual([row["status"] for row in rows],
                             ["generated", "acknowledged", "scheduling_failed"])
            self.assertAlmostEqual(rows[1]["send_to_ack_seconds"], 0.6)
            self.assertGreater(rows[2]["monotonic_lag_seconds"], 60 / 360)

    def test_600ms_fsync_blocks_publish_and_cannot_be_hidden_by_async_delivery(self):
        with TemporaryDirectory() as folder:
            published, error = self.run_with_clock(folder, sync_seconds=0.6)
            self.assertIsInstance(error, RuntimeError)
            self.assertEqual(len(published), 1)
            first_publish, event = published[0]
            timestamp = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
            self.assertEqual(timestamp, datetime(2026, 11, 4, tzinfo=timezone.utc))
            self.assertAlmostEqual(first_publish, 0.6)
            rows = [json.loads(line) for line in
                    (Path(folder) / "manifest.jsonl").read_text().splitlines()]
            self.assertEqual(rows[-1]["status"], "scheduling_failed")
            self.assertAlmostEqual(rows[-1]["previous_manifest_timing"]["fsync_seconds"], 0.6)

    def test_generated_writer_failure_prevents_publish(self):
        with TemporaryDirectory() as folder:
            published, error = self.run_with_clock(folder, sync_failure=1)
            self.assertIsInstance(error, OSError)
            self.assertEqual(published, [])

    def test_ack_writer_failure_stops_before_another_publish(self):
        with TemporaryDirectory() as folder:
            published, error = self.run_with_clock(folder, sync_failure=2)
            self.assertIsInstance(error, OSError)
            self.assertEqual(len(published), 1)
            # A readable line after failed fsync is not proof of durable acknowledgement.

    def test_client_queue_saturation_is_bounded_and_never_acknowledged(self):
        calls = []

        class SaturatedProducer:
            def produce(self, *args, **kwargs):
                calls.append("produce")
                raise BufferError("local queue full")

        with TemporaryDirectory() as folder:
            path = Path(folder) / "manifest.jsonl"
            event = generator.build_minute(360, "run", random.Random(1))[0]
            self.assertFalse(generator.publish_event(
                SaturatedProducer(), "topic", event, path, sleep=lambda seconds: None))
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(len(calls), 2)
            self.assertTrue(all(row["status"] == "failed" for row in rows))
            self.assertTrue(all(row["error_type"] == "BufferError" for row in rows))

    def test_restart_does_not_replay_unresolved_generated_identity(self):
        with TemporaryDirectory() as folder:
            first, error = self.run_with_clock(folder, stop_after_publish=True)
            self.assertIsInstance(error, KeyboardInterrupt)
            second, error = self.run_with_clock(folder, stop_after_publish=True)
            self.assertIsInstance(error, KeyboardInterrupt)
            self.assertNotEqual(first[0][1]["message_id"], second[0][1]["message_id"])
            rows = [json.loads(line) for line in
                    (Path(folder) / "manifest.jsonl").read_text().splitlines()]
            self.assertEqual([row["status"] for row in rows], ["generated", "generated"])
            self.assertEqual({row["run_id"] for row in rows}, {"same-run"})
            # Recovery evidence exists, but run() has no automatic outbox replay.

    def test_delivery_retry_preserves_exact_bytes_identity_timestamp_and_mix(self):
        events = generator.build_minute(360, "run", random.Random(7))
        self.assertEqual(sum(event["temperature"] > 80 for event in events), 36)
        event = events[0]
        payloads = []

        class RetryProducer:
            callback = None

            def produce(self, topic, key, value, on_delivery):
                payloads.append((key, value))
                self.callback = on_delivery

            def flush(self, timeout):
                self.callback("DeliveryTimeout" if len(payloads) == 1 else None, object())

        with TemporaryDirectory() as folder:
            path = Path(folder) / "manifest.jsonl"
            self.assertTrue(generator.publish_event(
                RetryProducer(), "topic", event, path, sleep=lambda seconds: None))
            self.assertEqual(payloads[0], payloads[1])
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([row["status"] for row in rows], ["failed", "acknowledged"])
            self.assertEqual(rows[-1]["timestamp"], event["timestamp"])
            self.assertEqual(rows[-1]["attempts"], 2)


if __name__ == "__main__":
    unittest.main()
