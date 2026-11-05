"""Reconcile per-message evidence from generator and Fabric table exports."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Iterable


def _key(record: dict) -> str | None:
    if not record.get("run_id") or not record.get("message_id"):
        return None
    return f"{record['run_id']}:{record['message_id']}"


def _as_utc(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an RFC3339 timestamp string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def _rows(records: Iterable[dict]) -> tuple[set[str], int, list[str], int]:
    keys: list[str] = []
    unkeyed = 0
    for record in records:
        key = _key(record)
        if key is None:
            unkeyed += 1
        else:
            keys.append(key)
    counts = Counter(keys)
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    return set(keys), len(records), duplicates, unkeyed


def reconcile(
    generated: list[dict],
    acknowledged: list[dict],
    raw: list[dict],
    events: list[dict],
    scores: list[dict],
    quarantine: list[dict],
    send_failed: list[dict] | None = None,
    audit: list[dict] | None = None,
    expected_generated: int | None = None,
    expected_high: int | None = None,
    max_events_latency_seconds: float = 20.0,
    max_scores_latency_seconds: float = 30.0,
    require_full_scoring: bool = True,
    latency_timestamp_field: str = "independently_read_at",
) -> dict:
    if latency_timestamp_field not in {"independently_read_at", "spark_processed_at"}:
        raise ValueError("Unsupported latency timestamp field")
    groups = {
        "generated": generated,
        "send_acknowledged": acknowledged,
        "send_failed": send_failed or [],
        "raw": raw,
        "events": events,
        "scores": scores,
        "quarantine": quarantine,
    }
    keysets: dict[str, set[str]] = {}
    physical: dict[str, int] = {}
    duplicates: dict[str, list[str]] = {}
    unkeyed: dict[str, int] = {}
    for name, records in groups.items():
        keysets[name], physical[name], duplicates[name], unkeyed[name] = _rows(records)

    raw_by_key: dict[str, dict] = {}
    for row in raw:
        if _key(row):
            raw_by_key.setdefault(_key(row), row)
    event_by_key: dict[str, dict] = {}
    for row in events:
        if _key(row):
            event_by_key.setdefault(_key(row), row)
    score_by_key: dict[str, dict] = {}
    for row in scores:
        if _key(row):
            score_by_key.setdefault(_key(row), row)
    quarantine_by_key: dict[str, list[dict]] = {}
    for row in quarantine:
        if _key(row):
            quarantine_by_key.setdefault(_key(row), []).append(row)

    generated_high_keys = {
        _key(row)
        for row in generated
        if _key(row) and float(row.get("temperature", -1)) > 80.0
    }
    raw_high_keys = {
        _key(row)
        for row in raw
        if _key(row) and float(row.get("temperature", -1)) > 80.0
    }
    high_keys = generated_high_keys
    scored = keysets["events"] & keysets["scores"]
    api_failed = {
        key
        for key in high_keys & (keysets["events"] - keysets["scores"])
        if any(str(row.get("stage", "")).upper() == "API" for row in quarantine_by_key.get(key, []))
    }
    pre_enrichment_failed = {
        key for key in high_keys - keysets["events"]
        if any(str(row.get("stage", "")).upper() != "API" for row in quarantine_by_key.get(key, []))
    }
    resolved = (scored | api_failed | pre_enrichment_failed) & high_keys
    unresolved = high_keys - resolved
    orphan_scores = keysets["scores"] - keysets["events"]
    unclassified = (keysets["events"] - keysets["scores"] - api_failed) & high_keys
    non_high_outputs = (keysets["events"] | keysets["scores"]) - high_keys
    missing_acknowledged = keysets["generated"] - keysets["send_acknowledged"]
    missing_raw = keysets["generated"] - keysets["raw"]
    unexpected_raw = keysets["raw"] - keysets["generated"]
    missing_high_raw = generated_high_keys - raw_high_keys
    unexpected_high_raw = raw_high_keys - generated_high_keys

    latency = {"events": [], "scores": []}
    row_mismatches = []
    for key, output in event_by_key.items():
        source = raw_by_key.get(key) or next((r for r in generated if _key(r) == key), None)
        if source:
            for output_field, source_field in (
                ("equipment_id", "equipment_id"),
                ("message_timestamp", "timestamp"),
            ):
                if output_field not in output or source_field not in source:
                    continue
                if output_field == "equipment_id":
                    mismatch = output[output_field] != source[source_field]
                else:
                    mismatch = _as_utc(output[output_field], output_field) != _as_utc(source[source_field], source_field)
                if mismatch:
                    row_mismatches.append({"key": key, "field": output_field})
        readable = output.get(latency_timestamp_field)
        if readable:
            if not source or not source.get("timestamp"):
                row_mismatches.append({"key": key, "field": "source_timestamp"})
                continue
            latency["events"].append(
                (_as_utc(readable, f"events.{latency_timestamp_field}") - _as_utc(
                    source["timestamp"], "source.timestamp"
                )).total_seconds()
            )
    for key, output in score_by_key.items():
        event_row = event_by_key.get(key)
        if event_row:
            for field in ("equipment_id", "message_timestamp"):
                if field in output and field in event_row and output[field] != event_row[field]:
                    if field == "message_timestamp":
                        mismatch = _as_utc(output[field], "score.message_timestamp") != _as_utc(
                            event_row[field], "events.message_timestamp"
                        )
                    else:
                        mismatch = output[field] != event_row[field]
                    if mismatch:
                        row_mismatches.append({"key": key, "field": f"score.{field}"})
        if isinstance(output.get("score"), bool) or not isinstance(output.get("score"), int) or not 1 <= output["score"] <= 100:
            row_mismatches.append({"key": key, "field": "score"})
        readable = output.get(latency_timestamp_field)
        if readable:
            source = raw_by_key.get(key) or next((r for r in generated if _key(r) == key), None)
            if not source or not source.get("timestamp"):
                row_mismatches.append({"key": key, "field": "source_timestamp"})
                continue
            latency["scores"].append(
                (_as_utc(readable, f"scores.{latency_timestamp_field}") - _as_utc(
                    source["timestamp"], "source.timestamp"
                )).total_seconds()
            )

    audit_rows = audit or []
    api_attempted = {
        _key(row)
        for row in audit_rows
        if _key(row) and int(row.get("api_attempts", 0) or 0) > 0
    }
    api_attempt_count = sum(
        int(row.get("api_attempts", 0) or 0)
        for row in audit_rows
        if _key(row)
    )
    attempts_by_key: dict[str, int] = {}
    for row in acknowledged:
        key = _key(row)
        if key:
            attempts_by_key[key] = max(attempts_by_key.get(key, 0), int(row.get("attempts", 0) or 0))
    for row in send_failed or []:
        key = _key(row)
        if key:
            attempts_by_key[key] = max(
                attempts_by_key.get(key, 0),
                int(row.get("attempts", row.get("attempt", 0)) or 0),
            )
    send_attempt_count = sum(attempts_by_key.values())
    failures = []
    if expected_generated is not None and len(keysets["generated"]) != expected_generated:
        failures.append("generated_count")
    if expected_high is not None and len(high_keys) != expected_high:
        failures.append("high_temperature_count")
    if duplicates["generated"] or duplicates["events"] or duplicates["scores"]:
        failures.append("duplicate_output_or_generated_key")
    if any(unkeyed[name] for name in ("generated", "send_acknowledged", "raw", "events", "scores")):
        failures.append("unkeyed_required_record")
    if keysets["send_acknowledged"] - keysets["generated"]:
        failures.append("acknowledgement_without_generated_record")
    if missing_acknowledged:
        failures.append("generated_without_send_acknowledgement")
    if unexpected_raw:
        failures.append("raw_without_generated_record")
    if missing_raw:
        failures.append("generated_missing_from_raw")
    if missing_high_raw or unexpected_high_raw:
        failures.append("high_trigger_missing_from_raw")
    if orphan_scores or unclassified or unresolved or non_high_outputs:
        failures.append("unresolved_or_inconsistent_outcomes")
    if require_full_scoring and keysets["scores"] != high_keys:
        failures.append("baseline_score_set_mismatch")
    if row_mismatches:
        failures.append("output_row_mismatch")
    if any(value < 0 for values in latency.values() for value in values):
        failures.append("negative_latency")
    if any(value > max_events_latency_seconds for value in latency["events"]):
        failures.append("events_latency_exceeded")
    if any(value > max_scores_latency_seconds for value in latency["scores"]):
        failures.append("scores_latency_exceeded")
    if len(latency["events"]) != len(events):
        failures.append("events_missing_independent_read_time")
    if len(latency["scores"]) != len(scores):
        failures.append("scores_missing_independent_read_time")

    return {
        "pass": not failures,
        "failures": sorted(set(failures)),
        "physical_rows": physical,
        "distinct_keys": {name: len(keys) for name, keys in keysets.items()},
        "duplicate_keys": duplicates,
        "unkeyed_records": unkeyed,
        "high_temperature_keys": len(high_keys),
        "api_attempted_keys": len(api_attempted),
        "api_attempt_count": api_attempt_count,
        "send_attempt_count": send_attempt_count,
        "send_failure_rows": physical["send_failed"],
        "send_failed_keys": sorted(keysets["send_failed"]),
        "missing_acknowledged": sorted(missing_acknowledged),
        "missing_raw": sorted(missing_raw),
        "missing_high_raw": sorted(missing_high_raw),
        "unexpected_high_raw": sorted(unexpected_high_raw),
        "outcomes": {
            "scored": sorted(scored),
            "api_failed_with_event": sorted(api_failed),
            "pre_enrichment_failed": sorted(pre_enrichment_failed),
            "unresolved": sorted(unresolved),
            "orphan_scores": sorted(orphan_scores),
            "non_high_outputs": sorted(non_high_outputs),
        },
        "latency_seconds": {
            "events": _latency_summary(latency["events"]),
            "scores": _latency_summary(latency["scores"]),
        },
        "row_mismatches": row_mismatches,
    }


def _latency_summary(values: list[float]) -> dict:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "max": None, "p50": None, "p95": None}

    def percentile(p: float) -> float:
        return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))]

    return {
        "count": len(ordered),
        "max": max(ordered),
        "p50": percentile(0.50),
        "p95": percentile(0.95),
    }


def _load_rows(folder: Path, name: str) -> list[dict]:
    path = folder / f"{name}.json"
    if not path.exists():
        return []
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise ValueError(f"{path} must contain a JSON array of objects")
    return value


def _load_generator_manifest(path: Path) -> tuple[list[dict], list[dict], list[dict]]:
    generated: list[dict] = []
    acknowledged: list[dict] = []
    failed: list[dict] = []
    with path.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"{path}:{line_number} is not valid JSON") from error
            if not isinstance(record, dict):
                raise ValueError(f"{path}:{line_number} must be a JSON object")
            status = record.get("status")
            if status == "generated":
                generated.append(record)
            elif status == "acknowledged":
                acknowledged.append(record)
            elif status == "failed":
                failed.append(record)
            else:
                raise ValueError(f"{path}:{line_number} has unsupported status {status!r}")
    return generated, acknowledged, failed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--expected-generated", type=int, default=900)
    parser.add_argument("--expected-high", type=int, default=90)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    generated = _load_rows(args.evidence_dir, "generated")
    acknowledged = _load_rows(args.evidence_dir, "acknowledged")
    send_failed = _load_rows(args.evidence_dir, "send_failed")
    if args.manifest:
        generated, acknowledged, send_failed = _load_generator_manifest(args.manifest)
    result = reconcile(
        generated=generated,
        acknowledged=acknowledged,
        send_failed=send_failed,
        raw=_load_rows(args.evidence_dir, "raw"),
        events=_load_rows(args.evidence_dir, "events"),
        scores=_load_rows(args.evidence_dir, "scores"),
        quarantine=_load_rows(args.evidence_dir, "quarantine"),
        audit=_load_rows(args.evidence_dir, "audit"),
        expected_generated=args.expected_generated,
        expected_high=args.expected_high,
    )
    output = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    print(output)
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
