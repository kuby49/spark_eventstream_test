"""Paced Kafka producer for the approved Eventstream source contract."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import random
import queue
import threading
import time
from typing import Callable
from uuid import uuid4

LOGGER = logging.getLogger("esehspark.generator")


def validate_rate(rate: int) -> None:
    if isinstance(rate, bool) or rate <= 0 or rate % 10:
        raise ValueError("rate must be a positive whole number divisible by 10")


def build_minute(rate: int, run_id: str, rng: random.Random,
                 minute_start: datetime | None = None) -> list[dict]:
    validate_rate(rate)
    high_count = rate // 10
    events: list[dict] = []
    minute_start = minute_start or datetime.now(timezone.utc)
    interval_seconds = 60.0 / rate
    for index in range(rate):
        high = index < high_count
        temperature = (
            rng.uniform(80.000001, 100.0)
            if high
            else rng.uniform(0.0, 80.0)
        )
        events.append(
            {
                "message_id": str(uuid4()),
                "run_id": run_id,
                "equipment_id": rng.randint(0, 100),
                "temperature": round(temperature, 6),
            }
        )
    rng.shuffle(events)
    for index, event in enumerate(events):
        event["timestamp"] = (
            minute_start + timedelta(seconds=index * interval_seconds)
        ).isoformat().replace("+00:00", "Z")
        event["payload"] = dict(event)
    return events


def _load_source_connection() -> tuple[str, str, str]:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    vault_url = os.environ["KEY_VAULT_URL"]
    secret_name = os.environ.get("EVENTSTREAM_SOURCE_SECRET", "eventstream-source-connection")
    secret = SecretClient(
        vault_url=vault_url,
        credential=DefaultAzureCredential(exclude_interactive_browser_credential=True),
    ).get_secret(secret_name).value
    parts = dict(
        segment.split("=", 1)
        for segment in secret.split(";")
        if "=" in segment
    )
    endpoint = parts.get("Endpoint", "")
    topic = parts.get("EntityPath", "")
    if not endpoint.startswith("sb://") or not topic or not parts.get("SharedAccessKey"):
        raise RuntimeError("Eventstream source connection secret has an unsupported format")
    bootstrap = f"{endpoint[5:].rstrip('/')} :9093".replace(" ", "")
    return bootstrap, topic, secret


class DurableManifest:
    def __init__(self, path: Path):
        self.path = path
        self.file = None
        self.last_timing = None
        self.maximum_write_seconds = 0.0
        self.maximum_flush_seconds = 0.0
        self.maximum_fsync_seconds = 0.0
        self.lock = threading.Lock()

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open("a", encoding="utf-8")
        return self

    def write(self, entry: dict) -> None:
        with self.lock:
            self._write(entry)

    def _write(self, entry: dict) -> None:
        started = time.monotonic()
        self.file.write(json.dumps(entry, separators=(",", ":")) + "\n")
        written = time.monotonic()
        self.file.flush()
        flushed = time.monotonic()
        os.fsync(self.file.fileno())
        synced = time.monotonic()
        self.last_timing = {
            "status": entry["status"], "write_seconds": written - started,
            "flush_seconds": flushed - written, "fsync_seconds": synced - flushed,
            "total_seconds": synced - started,
        }
        self.maximum_write_seconds = max(self.maximum_write_seconds, written - started)
        self.maximum_flush_seconds = max(self.maximum_flush_seconds, flushed - written)
        self.maximum_fsync_seconds = max(self.maximum_fsync_seconds, synced - flushed)

    def __exit__(self, error_type, error, traceback):
        self.file.close()


class BufferedManifest(DurableManifest):
    """Bounded diagnostic logging; durability is established at final close."""

    buffered = True

    def __init__(self, path):
        super().__init__(path)
        self.records = queue.Queue(maxsize=4096)
        self.stop = threading.Event()
        self.error = None
        self.writer = threading.Thread(target=self._consume, name="manifest-writer")

    def __enter__(self):
        super().__enter__()
        self.writer.start()
        return self

    def write(self, entry):
        if self.error is not None:
            raise RuntimeError("Buffered manifest writer failed") from self.error
        try:
            self.records.put_nowait(json.dumps(entry, separators=(",", ":")) + "\n")
        except queue.Full as error:
            raise RuntimeError("Buffered manifest queue saturated") from error

    def _consume(self):
        try:
            last_flush = time.monotonic()
            while not self.stop.is_set() or not self.records.empty():
                try:
                    line = self.records.get(timeout=0.1)
                except queue.Empty:
                    line = None
                if line is not None:
                    self.file.write(line)
                if time.monotonic() - last_flush >= 1:
                    self.file.flush()
                    last_flush = time.monotonic()
            self.file.flush()
            os.fsync(self.file.fileno())
        except Exception as error:
            self.error = error

    def __exit__(self, error_type, error, traceback):
        self.stop.set()
        self.writer.join(timeout=15)
        if self.writer.is_alive():
            raise RuntimeError("Buffered manifest writer did not finish in bound")
        self.file.close()
        if self.error is not None:
            raise RuntimeError("Buffered manifest writer failed") from self.error


def _write_manifest(path: Path | DurableManifest, entry: dict) -> None:
    if isinstance(path, DurableManifest):
        path.write(entry)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(entry, separators=(",", ":")) + "\n")
        file.flush()
        os.fsync(file.fileno())


def generated_manifest_record(event: dict) -> dict:
    return {
        "message_id": event["message_id"],
        "run_id": event["run_id"],
        "timestamp": event["timestamp"],
        "equipment_id": event["equipment_id"],
        "temperature": event["temperature"],
        "payload": event,
        "status": "generated",
    }


def publish_event(
    producer,
    topic: str,
    event: dict,
    manifest: Path,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    payload = json.dumps(event, separators=(",", ":"), allow_nan=False).encode("utf-8")
    outcomes: list[tuple[object, object]] = []
    for attempt in (1, 2):
        outcomes.clear()

        def delivered(error, message):
            outcomes.append((error, message))

        attempted_at = time.monotonic()
        enqueue_started_at = datetime.now(timezone.utc).isoformat()
        try:
            producer.produce(
                topic,
                key=event["message_id"].encode("ascii"),
                value=payload,
                on_delivery=delivered,
            )
            queued_at = time.monotonic()
            producer.flush(8)
            flushed_at = time.monotonic()
        except Exception as exc:
            outcomes.append((type(exc).__name__, None))

        if outcomes and outcomes[-1][0] is None:
            _write_manifest(
                manifest,
                {
                    "message_id": event["message_id"],
                    "run_id": event["run_id"],
                    "timestamp": event["timestamp"],
                    "attempts": attempt,
                    "status": "acknowledged",
                    "acknowledged_at": datetime.now(timezone.utc).isoformat(),
                    "enqueue_started_at": enqueue_started_at,
                    "enqueue_seconds": queued_at - attempted_at,
                    "delivery_flush_seconds": flushed_at - queued_at,
                    "send_to_ack_seconds": flushed_at - attempted_at,
                    "previous_manifest_timing": (
                        manifest.last_timing if isinstance(manifest, DurableManifest) else None),
                },
            )
            return True
        _write_manifest(
            manifest,
            {
                "message_id": event["message_id"],
                "run_id": event["run_id"],
                "attempt": attempt,
                "status": "failed",
                "error_type": (
                    type(outcomes[-1][0]).__name__
                    if outcomes and isinstance(outcomes[-1][0], BaseException)
                    else str(outcomes[-1][0])[:80] if outcomes else "NoDeliveryReport"
                ),
                "failed_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        if attempt == 1:
            sleep(random.uniform(0.05, 0.2))

    _write_manifest(
        manifest,
        {
            "message_id": event["message_id"],
            "run_id": event["run_id"],
            "timestamp": event["timestamp"],
            "attempts": 2,
            "status": "failed",
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "error_type": str(outcomes[-1][0])[:80] if outcomes else "NoDeliveryReport",
        },
    )
    return False


class BackgroundDelivery:
    """Bounded delivery tracking; generated records must already be durable."""

    def __init__(self, producer, topic, manifest, run_id, maximum_inflight=64):
        self.producer = producer
        self.topic = topic
        self.manifest = manifest
        self.run_id = run_id
        self.maximum_inflight = maximum_inflight
        self.slots = threading.BoundedSemaphore(maximum_inflight)
        self.pending = {}
        self.lock = threading.Lock()
        self.errors = []
        self.failed = 0
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self._poll, name="delivery-tracker")

    def start(self):
        self.thread.start()

    def check(self):
        if getattr(self.manifest, "error", None) is not None:
            raise RuntimeError("Buffered manifest writer failed") from self.manifest.error
        with self.lock:
            error = self.errors[0] if self.errors else None
        if error is not None:
            raise RuntimeError("Background delivery tracking failed") from error

    def _record_error(self, error):
        with self.lock:
            self.errors.append(error)

    def _poll(self):
        while not self.stopping.is_set():
            try:
                self.producer.poll(0.05)
            except Exception as error:
                self._record_error(error)
                return

    def submit(self, event):
        self.check()
        if not self.slots.acquire(blocking=False):
            raise RuntimeError("Producer bounded inflight queue saturated")
        key = event["message_id"]
        state = {
            "event": event,
            "payload": json.dumps(event, separators=(",", ":"), allow_nan=False).encode("utf-8"),
            "attempt": 0,
        }
        with self.lock:
            self.pending[key] = state
        self._enqueue(key, state)
        self.check()

    def _enqueue(self, key, state):
        state["attempt"] += 1
        state["started"] = time.monotonic()
        state["enqueue_started_at"] = datetime.now(timezone.utc).isoformat()
        if state["attempt"] == 1:
            state["first_enqueue_started_at"] = state["enqueue_started_at"]
        try:
            self.producer.produce(
                self.topic, key=key.encode("ascii"), value=state["payload"],
                on_delivery=lambda error, message: self._delivered(key, state, error))
        except Exception as error:
            self._record_error(error)
            # Keep the durable generated identity unresolved; do not pretend it was delivered.

    def _delivered(self, key, state, error):
        try:
            event = state["event"]
            if error is not None:
                _write_manifest(self.manifest, {
                    "status": "failed", "run_id": self.run_id, "message_id": key,
                    "attempt": state["attempt"], "error_type": type(error).__name__,
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                })
                if state["attempt"] < 2:
                    self._enqueue(key, state)
                    return
                self.failed += 1
                self._record_error(RuntimeError("Producer delivery failed after two attempts"))
            else:
                _write_manifest(self.manifest, {
                    "status": "acknowledged", "run_id": self.run_id, "message_id": key,
                    "timestamp": event["timestamp"], "attempts": state["attempt"],
                    "acknowledged_at": datetime.now(timezone.utc).isoformat(),
                    "enqueue_started_at": state["enqueue_started_at"],
                    "first_enqueue_started_at": state["first_enqueue_started_at"],
                    "send_to_ack_seconds": time.monotonic() - state["started"],
                })
            with self.lock:
                del self.pending[key]
            self.slots.release()
        except Exception as failure:
            self._record_error(failure)

    def finish(self, timeout=20):
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline:
                with self.lock:
                    pending = bool(self.pending)
                if not pending:
                    break
                self.stopping.wait(0.05)
        finally:
            self.stopping.set()
            self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise RuntimeError("Background delivery tracker did not stop in bound")
        self.check()
        with self.lock:
            unresolved = list(self.pending)
        if unresolved:
            raise RuntimeError(f"Producer drain deadline exceeded; unresolved={len(unresolved)}")
        return self.failed


def run(rate: int, minutes: int, run_id: str, manifest: Path,
        buffered_manifest: bool = False) -> int:
    manifest_type = BufferedManifest if buffered_manifest else DurableManifest
    with manifest_type(manifest) as durable:
        return _run(rate, minutes, run_id, durable)


def _run(rate: int, minutes: int, run_id: str, manifest: DurableManifest) -> int:
    validate_rate(rate)
    if minutes <= 0:
        raise ValueError("minutes must be positive")
    from confluent_kafka import Producer

    bootstrap, topic, connection = _load_source_connection()
    parts = dict(segment.split("=", 1) for segment in connection.split(";") if "=" in segment)
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": connection,
            "client.id": f"esehspark-{run_id}",
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 8000,
            "retries": 1,
            "queue.buffering.max.messages": 64,
            "queue.buffering.max.kbytes": 1024,
        }
    )
    del parts, connection
    metadata = producer.list_topics(topic=topic, timeout=15)
    if topic not in metadata.topics or metadata.topics[topic].error is not None:
        raise RuntimeError("Producer topic metadata unavailable before measured sends")
    delivery = BackgroundDelivery(producer, topic, manifest, run_id)
    delivery.start()
    try:
        maximum_scheduling_lag = _send_schedule(rate, minutes, run_id, manifest, delivery)
    finally:
        failed = delivery.finish()
    _write_manifest(manifest, {
        "status": "producer_timing", "run_id": run_id,
        "maximum_scheduling_lag_seconds": maximum_scheduling_lag,
        "maximum_manifest_write_seconds": manifest.maximum_write_seconds,
        "maximum_manifest_flush_seconds": manifest.maximum_flush_seconds,
        "maximum_manifest_fsync_seconds": manifest.maximum_fsync_seconds,
        "maximum_inflight": delivery.maximum_inflight,
        "manifest_durability": (
            "buffered_diagnostic_log_final_fsync_crash_loss_possible"
            if getattr(manifest, "buffered", False)
            else "generated_before_publish_fsync_bounded_background_ack_fsync"),
    })
    return failed


def _send_schedule(rate, minutes, run_id, manifest, delivery):
    start = time.monotonic()
    timestamp_start = datetime.now(timezone.utc)
    interval = 60.0 / rate
    maximum_scheduling_lag = 0.0
    next_allowed = start
    for minute in range(minutes):
        minute_events = build_minute(
            rate, run_id, random.Random(f"{run_id}:{minute}"),
            minute_start=timestamp_start + timedelta(minutes=minute))
        for index, event in enumerate(minute_events):
            delivery.check()
            due = start + minute * 60 + index * interval
            if getattr(manifest, "buffered", False):
                due = max(due, next_allowed)
            scheduled_at = event["timestamp"]
            scheduled_utc = datetime.fromisoformat(scheduled_at.replace("Z", "+00:00"))
            while True:
                # UTC can slew relative to monotonic time; neither clock may send early.
                send_time = datetime.now(timezone.utc)
                delay = max(due - time.monotonic(),
                            (scheduled_utc - send_time).total_seconds())
                if delay <= 0:
                    break
                time.sleep(delay)
            if not getattr(manifest, "buffered", False) and (time.monotonic() - due > interval
                    or (send_time - scheduled_utc).total_seconds() > interval):
                _write_manifest(manifest, {
                    "status": "scheduling_failed", "run_id": run_id,
                    "scheduled_minute": minute, "scheduled_index": index,
                    "scheduled_at": scheduled_at, "observed_at": send_time.isoformat(),
                    "monotonic_lag_seconds": time.monotonic() - due,
                    "utc_lag_seconds": (send_time - scheduled_utc).total_seconds(),
                    "interval_seconds": interval,
                    "previous_manifest_timing": manifest.last_timing,
                })
                raise RuntimeError(
                    "Producer exceeded one event interval; aborting rather than "
                    "compressing late sends into the next minute"
                )
            event["timestamp"] = send_time.isoformat().replace("+00:00", "Z")
            event["payload"]["timestamp"] = event["timestamp"]
            _write_manifest(manifest, dict(
                generated_manifest_record(event), scheduled_minute=minute,
                scheduled_index=index, scheduled_at=scheduled_at,
                scheduling_lag_seconds=(send_time - scheduled_utc).total_seconds(),
                monotonic_scheduling_lag_seconds=time.monotonic() - due,
                schedule_wake_at=send_time.isoformat(),
                previous_manifest_timing=manifest.last_timing))
            delivery.check()
            maximum_scheduling_lag = max(
                maximum_scheduling_lag, (send_time - scheduled_utc).total_seconds())
            delivery.submit(event)
            next_allowed = time.monotonic() + interval
        LOGGER.info(
            "minute=%s run_id=%s generated=%s high=%s",
            minute + 1,
            run_id,
            rate,
            rate // 10,
        )
    return maximum_scheduling_lag


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=30, help="events per minute; positive multiple of 10")
    parser.add_argument("--minutes", type=int, default=30)
    parser.add_argument("--run-id", default=str(uuid4()))
    parser.add_argument("--buffered-manifest", action="store_true",
                        help="buffer diagnostic logs; crash may lose unsaved entries")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("logs") / f"generator-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.jsonl",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    failed = run(args.rate, args.minutes, args.run_id, args.manifest, args.buffered_manifest)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
