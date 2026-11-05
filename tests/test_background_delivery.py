from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
import sys
from tempfile import TemporaryDirectory
import threading
import types
import unittest
from unittest.mock import patch

from src.generator import producer as generator


class QueuedProducer:
    def __init__(self):
        self.calls = []
        self.reports = []
        self.lock = threading.Lock()

    def produce(self, topic, key, value, on_delivery):
        with self.lock:
            self.calls.append((key, value))
            self.reports.append(on_delivery)

    def poll(self, timeout):
        with self.lock:
            report = self.reports.pop(0) if self.reports else None
        if report:
            report(None, object())
        else:
            threading.Event().wait(min(timeout, 0.001))


class BackgroundDeliveryTests(unittest.TestCase):
    def event(self, index=0):
        return generator.build_minute(360, "run", random.Random(7))[index]

    def test_sends_before_prior_ack_and_finish_durably_tracks_all(self):
        with TemporaryDirectory() as folder:
            path = Path(folder) / "manifest.jsonl"
            producer = QueuedProducer()
            with generator.DurableManifest(path) as manifest:
                delivery = generator.BackgroundDelivery(producer, "topic", manifest, "run")
                for event in (self.event(0), self.event(1)):
                    manifest.write(generator.generated_manifest_record(event))
                    delivery.submit(event)
                self.assertEqual(len(producer.calls), 2)
                self.assertEqual(len(delivery.pending), 2)
                self.assertEqual(len(path.read_text().splitlines()), 2)
                delivery.start()
                self.assertEqual(delivery.finish(), 0)
                self.assertFalse(delivery.thread.is_alive())
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(sum(row["status"] == "acknowledged" for row in rows), 2)

    def test_full_minute_with_600ms_delivery_keeps_schedule_and_exact_mix(self):
        clock = [0.0]
        origin = datetime(2026, 11, 4, tzinfo=timezone.utc)
        calls, pending = [], []

        class ClockDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return origin + timedelta(seconds=clock[0])

        class Producer:
            def produce(self, topic, key, value, on_delivery):
                calls.append((clock[0], json.loads(value)))
                pending.append((clock[0] + 0.6, on_delivery))

            def poll(self, timeout):
                while pending and pending[0][0] <= clock[0]:
                    _, callback = pending.pop(0)
                    callback(None, object())

        producer = Producer()

        def sleep(seconds):
            clock[0] += seconds
            producer.poll(0)

        with TemporaryDirectory() as folder, \
                patch.object(generator, "datetime", ClockDateTime), \
                patch.object(generator.time, "monotonic", lambda: clock[0]), \
                patch.object(generator.time, "sleep", sleep), \
                patch.object(generator.os, "fsync"):
            path = Path(folder) / "manifest.jsonl"
            with generator.DurableManifest(path) as manifest:
                delivery = generator.BackgroundDelivery(producer, "topic", manifest, "run")
                generator._send_schedule(360, 1, "run", manifest, delivery)
                clock[0] += 1
                producer.poll(0)
                self.assertEqual(delivery.pending, {})
            self.assertEqual(len(calls), 360)
            self.assertEqual(sum(event["temperature"] > 80 for _, event in calls), 36)
            for index, (sent, event) in enumerate(calls):
                self.assertAlmostEqual(sent, index / 6, places=5)
                self.assertEqual(event["payload"]["timestamp"], event["timestamp"])
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(sum(row["status"] == "acknowledged" for row in rows), 360)

    def test_inflight_saturation_stops_without_an_extra_enqueue(self):
        with TemporaryDirectory() as folder:
            producer = QueuedProducer()
            with generator.DurableManifest(Path(folder) / "manifest") as manifest:
                delivery = generator.BackgroundDelivery(
                    producer, "topic", manifest, "run", maximum_inflight=1)
                delivery.submit(self.event())
                with self.assertRaisesRegex(RuntimeError, "queue saturated"):
                    delivery.submit(self.event(1))
                self.assertEqual(len(producer.calls), 1)

    def test_ack_fsync_failure_propagates_and_keeps_identity_unresolved(self):
        with TemporaryDirectory() as folder:
            producer = QueuedProducer()
            with generator.DurableManifest(Path(folder) / "manifest") as manifest:
                delivery = generator.BackgroundDelivery(producer, "topic", manifest, "run")
                event = self.event()
                manifest.write(generator.generated_manifest_record(event))
                delivery.submit(event)
                with patch.object(generator.os, "fsync", side_effect=OSError("disk")):
                    producer.poll(0)
                with self.assertRaisesRegex(RuntimeError, "tracking failed"):
                    delivery.check()
                self.assertIn(event["message_id"], delivery.pending)
                with self.assertRaises(RuntimeError):
                    delivery.submit(self.event(1))
                self.assertEqual(len(producer.calls), 1)

    def test_retry_uses_identical_bytes_and_final_failure_is_explicit(self):
        with TemporaryDirectory() as folder:
            producer = QueuedProducer()
            with generator.DurableManifest(Path(folder) / "manifest") as manifest:
                delivery = generator.BackgroundDelivery(producer, "topic", manifest, "run")
                delivery.submit(self.event())
                first = producer.reports.pop()
                first(TimeoutError(), None)
                self.assertEqual(producer.calls[0], producer.calls[1])
                second = producer.reports.pop()
                second(TimeoutError(), None)
                with self.assertRaisesRegex(RuntimeError, "tracking failed"):
                    delivery.check()
                self.assertEqual(delivery.failed, 1)
                self.assertEqual(delivery.pending, {})

    def test_drain_timeout_retains_unresolved_and_stops_poller(self):
        producer = QueuedProducer()
        producer.poll = lambda timeout: threading.Event().wait(0.001)
        with TemporaryDirectory() as folder:
            with generator.DurableManifest(Path(folder) / "manifest") as manifest:
                delivery = generator.BackgroundDelivery(producer, "topic", manifest, "run")
                delivery.submit(self.event())
                delivery.start()
                with self.assertRaisesRegex(RuntimeError, "drain deadline"):
                    delivery.finish(timeout=0)
                self.assertFalse(delivery.thread.is_alive())
                self.assertEqual(len(delivery.pending), 1)

    def test_run_generated_fsync_failure_prevents_any_enqueue(self):
        producer = QueuedProducer()
        producer.list_topics = lambda **kwargs: types.SimpleNamespace(
            topics={"topic": types.SimpleNamespace(error=None)})
        kafka = types.SimpleNamespace(Producer=lambda config: producer)
        with TemporaryDirectory() as folder, \
                patch.dict(sys.modules, {"confluent_kafka": kafka}), \
                patch.object(generator, "_load_source_connection", return_value=(
                    "private:9093", "topic", "Endpoint=sb://private;EntityPath=topic")), \
                patch.object(generator.os, "fsync", side_effect=OSError("disk")):
            with self.assertRaisesRegex(OSError, "disk"):
                generator.run(390, 1, "run", Path(folder) / "manifest")
            self.assertEqual(producer.calls, [])

    def test_ack_slot_is_held_until_durable_write_completes(self):
        entered, release = threading.Event(), threading.Event()
        with TemporaryDirectory() as folder:
            producer = QueuedProducer()
            with generator.DurableManifest(Path(folder) / "manifest") as manifest:
                delivery = generator.BackgroundDelivery(
                    producer, "topic", manifest, "run", maximum_inflight=1)
                delivery.submit(self.event())
                def sync(descriptor):
                    entered.set()
                    if not release.wait(2):
                        raise TimeoutError("test release missing")
                with patch.object(generator.os, "fsync", sync):
                    delivery.start()
                    try:
                        self.assertTrue(entered.wait(2))
                        with self.assertRaisesRegex(RuntimeError, "queue saturated"):
                            delivery.submit(self.event(1))
                    finally:
                        release.set()
                        delivery.finish()
                self.assertEqual(delivery.pending, {})


if __name__ == "__main__":
    unittest.main()
