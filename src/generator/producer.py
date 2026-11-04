"""Paced Kafka producer for the approved Eventstream source contract."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import random
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


def _write_manifest(path: Path, entry: dict) -> None:
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

        try:
            producer.produce(
                topic,
                key=event["message_id"].encode("ascii"),
                value=payload,
                on_delivery=delivered,
            )
            producer.flush(8)
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


def run(rate: int, minutes: int, run_id: str, manifest: Path) -> int:
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
        }
    )
    del parts, connection
    failed = 0
    start = time.monotonic()
    timestamp_start = datetime.now(timezone.utc)
    interval = 60.0 / rate
    for minute in range(minutes):
        minute_events = build_minute(
            rate, run_id, random.Random(f"{run_id}:{minute}"),
            minute_start=timestamp_start + timedelta(minutes=minute))
        for index, event in enumerate(minute_events):
            due = start + minute * 60 + index * interval
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
            if (time.monotonic() - due > interval
                    or (send_time - scheduled_utc).total_seconds() > interval):
                raise RuntimeError(
                    "Producer exceeded one event interval; aborting rather than "
                    "compressing late sends into the next minute"
                )
            event["timestamp"] = send_time.isoformat().replace("+00:00", "Z")
            event["payload"]["timestamp"] = event["timestamp"]
            _write_manifest(manifest, dict(
                generated_manifest_record(event), scheduled_minute=minute,
                scheduled_index=index, scheduled_at=scheduled_at))
            if not publish_event(producer, topic, event, manifest):
                failed += 1
        LOGGER.info(
            "minute=%s run_id=%s generated=%s high=%s",
            minute + 1,
            run_id,
            rate,
            rate // 10,
        )
    producer.flush(10)
    return failed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=30, help="events per minute; positive multiple of 10")
    parser.add_argument("--minutes", type=int, default=30)
    parser.add_argument("--run-id", default=str(uuid4()))
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("logs") / f"generator-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.jsonl",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    failed = run(args.rate, args.minutes, args.run_id, args.manifest)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
