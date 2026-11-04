"""Evaluate complete baseline or latency-only load evidence without changing targets."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path

if __package__:
    from .reconcile import reconcile
else:
    from reconcile import reconcile


def _timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    # Spark TIMESTAMP exports are naive UTC under the explicit session timezone.
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def evaluate(report, rate=30, minutes=30, latency_only=False):
    manifest = report.get("generatorManifest") or []
    generated = [row for row in manifest if row.get("status") == "generated"]
    acknowledged = [row for row in manifest if row.get("status") == "acknowledged"]
    failed = [row for row in manifest if row.get("status") == "failed"]
    result = report.get("result") or {}
    outputs = result.get("outputRecords") or {}
    basis = report.get("latencyMeasurementBasis", "independent_delta_visibility")
    if basis not in {"independent_delta_visibility", "spark_pre_write"}:
        raise ValueError("Unsupported latency measurement basis")
    time_field = "spark_processed_at" if basis == "spark_pre_write" else "independently_read_at"
    if basis == "spark_pre_write":
        visible = [dict(row, table=table) for table in ("Events", "Scores")
                   for row in outputs.get(f"lhspark.{table}") or []]
        # Spark JSON TIMESTAMP values without an offset are UTC by session configuration.
        visible = [dict(row, **{field: _timestamp(row[field]).isoformat()
                               for field in ("message_timestamp", time_field) if row.get(field)})
                   for row in visible]
    else:
        visible = report.get("independentVisibility") or []
    events = [row for row in visible if row.get("table") == "Events"]
    scores = [row for row in visible if row.get("table") == "Scores"]
    expected_high = rate * minutes // 10
    evaluated = reconcile(
        generated, acknowledged, result.get("rawRecords") or [], events, scores,
        outputs.get("lhspark.Quarantine") or [], send_failed=failed,
        audit=outputs.get("lhspark.ProcessingAudit") or [],
        expected_generated=rate * minutes, expected_high=expected_high,
        latency_timestamp_field=time_field)
    failures = list(evaluated["failures"])
    if basis == "spark_pre_write" and any(not row.get(time_field) for row in visible):
        failures.append("spark_processing_timestamp_missing")
    averages = []
    generated_by_key = {(row["run_id"], row["message_id"]): row for row in generated}
    raw_publication = {"measured": bool(report.get("rawPublicationMeasured")), "records": []}
    if not latency_only and (raw_publication["measured"] or (rate == 30 and minutes == 30)):
        if not raw_publication["measured"]:
            failures.append("raw_publication_not_measured")
        observed_raw = report.get("independentRawVisibility") or []
        raw_keys = {(row.get("run_id"), row.get("message_id")) for row in observed_raw}
        if (report.get("independentRawObserverExitCode") != 0
                or raw_keys != set(generated_by_key) or len(observed_raw) != len(raw_keys)):
            failures.append("raw_publication_coverage_or_reader_failure")
        for row in observed_raw:
            source = generated_by_key.get((row.get("run_id"), row.get("message_id")))
            if source is None:
                failures.append("raw_publication_unexpected_identity")
                continue
            if (row.get("equipment_id") != source["equipment_id"]
                    or row.get("temperature") != source["temperature"]
                    or _timestamp(row["timestamp"]) != _timestamp(source["timestamp"])):
                failures.append("raw_publication_payload_mismatch")
            seconds = (_timestamp(row["independently_read_at"])
                       - _timestamp(source["timestamp"])).total_seconds()
            raw_publication["records"].append({
                "run_id": row["run_id"], "message_id": row["message_id"],
                "latencySeconds": seconds, "limitSeconds": 300, "pass": 0 <= seconds <= 300})
            if not 0 <= seconds <= 300:
                failures.append("raw_publication_latency_exceeded")
        raw_publication["maximumSeconds"] = max(
            (row["latencySeconds"] for row in raw_publication["records"]), default=None)
    latency_records = []
    for row in visible:
        source = generated_by_key.get((row.get("run_id"), row.get("message_id")))
        if source and row.get(time_field) and row.get("table") in ("Events", "Scores"):
            seconds = (_timestamp(row[time_field])
                       - _timestamp(source["timestamp"])).total_seconds()
            limit = 20 if row["table"] == "Events" else 30
            latency_records.append({
                "run_id": row["run_id"], "message_id": row["message_id"], "table": row["table"],
                "generatorTimestamp": source["timestamp"],
                "measurementBasis": basis, "measurementTimestamp": row[time_field],
                "latencySeconds": seconds, "limitSeconds": limit, "pass": 0 <= seconds <= limit})
    if not latency_only:
        raw = result.get("rawRecords") or []
        if len(raw) < rate * minutes:
            failures.append("raw_physical_missing")
        for records, label in ((raw, "raw"), (
                [row["event"] for row in result.get("derivedRecords") or []
                 if isinstance(row.get("event"), dict)], "derived")):
            for row in records:
                source = generated_by_key.get((row.get("run_id"), row.get("message_id")))
                if source is None or any(
                        row.get(field) != source.get(field)
                        for field in ("equipment_id", "temperature")) or (
                        _timestamp(row["timestamp"]) != _timestamp(source["timestamp"])):
                    failures.append(f"{label}_payload_mismatch")
        history = result.get("rawWindowRecords")
        if history is None:
            failures.append("average_history_missing")
        else:
            unique_history = {}
            for row in history:
                key = (row["run_id"], row["message_id"])
                if key in unique_history and unique_history[key] != row:
                    failures.append("average_history_conflicting_identity")
                unique_history[key] = row
            snapshots = {
                (timing["parameters"].get("runId"), timing["parameters"].get("messageId")): timing
                for timing in result.get("stageTimings") or []
                if timing.get("aggregateProjection") == [
                    "average", "unique_count", "trigger_visible", "window_records"]
                and isinstance(timing.get("parameters"), dict)}
            audit_by_key = {(row["run_id"], row["message_id"]): row for row in
                            outputs.get("lhspark.ProcessingAudit") or []}
            for event in outputs.get("lhspark.Events") or []:
                key = (event["run_id"], event["message_id"])
                trigger = _timestamp(event["message_timestamp"])
                window = [row for row in unique_history.values()
                          if row["equipment_id"] == event["equipment_id"]
                          and trigger - timedelta(minutes=20)
                          <= _timestamp(row["timestamp"]) <= trigger]
                final_expected = sum(row["temperature"] for row in window) / len(window) if window else None
                snapshot = snapshots.get(key)
                if not snapshot or len(snapshot.get("aggregateRows") or []) != 1:
                    failures.append("average_snapshot_missing")
                    continue
                parameters = snapshot["parameters"]
                if (parameters.get("equipmentId") != event["equipment_id"]
                        or _timestamp(parameters["triggerTime"]) != trigger):
                    failures.append("average_snapshot_parameters_mismatch")
                average, count, visible_at_query, members = snapshot["aggregateRows"][0]
                members = json.loads(members) if isinstance(members, str) else members
                if (count == 0 and visible_at_query == 0 and members == []
                        and (average == "NaN" or (type(average) is float and math.isnan(average)))):
                    average = None
                member_keys = {(row["run_id"], row["message_id"]) for row in members}
                if count != len(members) or len(member_keys) != len(members):
                    failures.append("average_snapshot_count_or_duplicates")
                for row in members:
                    original = unique_history.get((row["run_id"], row["message_id"]))
                    member_time = _timestamp(row["timestamp"])
                    if (not original or original["equipment_id"] != event["equipment_id"]
                            or row["temperature"] != original["temperature"]
                            or member_time != _timestamp(original["timestamp"])
                            or not trigger - timedelta(minutes=20) <= member_time <= trigger):
                        failures.append("average_snapshot_member_mismatch")
                computed_average = (sum(row["temperature"] for row in members) / len(members)
                                    if members else None)
                if ((average is None) != (computed_average is None) or (
                        average is not None and not math.isclose(
                            average, computed_average, abs_tol=0.000001, rel_tol=0.000000001))):
                    failures.append("kql_snapshot_average_mismatch")
                expected = computed_average if computed_average is not None else generated_by_key[key]["temperature"]
                matches = math.isclose(event["calculated_temperature"], expected,
                                       abs_tol=0.000001, rel_tol=0.000000001)
                audit = audit_by_key.get(key) or {}
                if (audit.get("query_unique_count") != count
                        or bool(audit.get("raw_visible_at")) != bool(visible_at_query)
                        or visible_at_query != int(key in member_keys)
                        or audit.get("fallback_reason") != (
                            "SuccessfulEmptyOrNullKqlResult" if average is None else None)):
                    failures.append("average_audit_snapshot_mismatch")
                averages.append({"run_id": event["run_id"], "message_id": event["message_id"],
                                 "equipment_id": event["equipment_id"], "snapshotCount": count,
                                 "expected": expected, "actual": event["calculated_temperature"],
                                 "matchesQuerySnapshot": matches,
                                 "triggerVisibleAtQuery": bool(visible_at_query),
                                 "finalRawWindowCount": len(window), "finalRawWindowAverage": final_expected})
                if not matches:
                    failures.append("calculated_average_mismatch")
    if report.get("generatorExitCode") != 0:
        failures.append("generator_not_successful")
    if basis == "independent_delta_visibility" and report.get("independentObserverExitCode") != 0:
        failures.append("independent_observer_not_successful")
    if result.get("error") or report.get("failedStage") or report.get("jobStatus") != "Completed":
        failures.append("execution_not_successful")
    for table in ("Events", "Scores"):
        physical = outputs.get(f"lhspark.{table}") or []
        if len(physical) != expected_high:
            failures.append(f"{table.lower()}_physical_count")
        keys = [(row["run_id"], row["message_id"]) for row in physical]
        if len(set(keys)) != len(keys):
            failures.append(f"{table.lower()}_physical_duplicates")
        observed_keys = {(row["run_id"], row["message_id"])
                         for row in visible if row.get("table") == table}
        if set(keys) != observed_keys:
            failures.append(f"{table.lower()}_visibility_key_mismatch")
        observed_by_key = {(row["run_id"], row["message_id"]): row for row in visible
                           if row.get("table") == table}
        value_field = "score" if table == "Scores" else "calculated_temperature"
        for row in physical:
            observed = observed_by_key.get((row["run_id"], row["message_id"]))
            if observed and (
                    row.get("equipment_id") != observed.get("equipment_id")
                    or row.get(value_field) != observed.get(value_field)
                    or _timestamp(row["message_timestamp"])
                    != _timestamp(observed["message_timestamp"])):
                failures.append(f"{table.lower()}_visibility_value_mismatch")
    derived = result.get("derivedRecords") or []
    generated_high = [row for row in generated if row["temperature"] > 80]
    high_keys = {(row["run_id"], row["message_id"]) for row in generated_high}
    derived_keys = {(row["event"]["run_id"], row["event"]["message_id"]) for row in derived
                    if isinstance(row.get("event"), dict)}
    if high_keys != derived_keys:
        failures.append("derived_key_mismatch")
    if len(derived_keys) != expected_high:
        failures.append("derived_count")
    if any(not isinstance(row.get("event"), dict)
           or not row["event"].get("run_id") or not row["event"].get("message_id") for row in derived):
        failures.append("derived_unkeyed_record")
    if not latency_only:
        audit = outputs.get("lhspark.ProcessingAudit") or []
        if any(not row.get("resolution") for row in outputs.get("lhspark.Quarantine") or []):
            failures.append("baseline_unresolved_quarantine")
        if len(audit) != expected_high or {
            (row["run_id"], row["message_id"]) for row in audit if row.get("status") == "SCORED"
        } != high_keys:
            failures.append("scored_audit_mismatch")
        physical_scores = {(row["run_id"], row["message_id"]): row for row in
                           outputs.get("lhspark.Scores") or []}
        api_timings = {(row.get("runId"), row.get("messageId")): row for row in
                       result.get("stageTimings") or [] if row.get("stage") == "score_api"}
        for row in audit:
            key = (row["run_id"], row["message_id"])
            timing = api_timings.get(key)
            if (row.get("api_attempts") not in (1, 2) or not timing
                    or timing.get("attempts") != row.get("api_attempts")
                    or timing.get("errorType") or timing.get("elapsedSeconds", 9) > 8):
                failures.append("api_attempts_or_total_budget")
            score = physical_scores.get(key)
            if score and row.get("api_result") != str(score["score"]):
                failures.append("api_audit_score_mismatch")
    if generated:
        start = min(_timestamp(row["timestamp"])
                    for row in generated)
        buckets = {}
        for row in generated:
            minute = row.get("scheduled_minute")
            if minute is None:
                minute = int((_timestamp(row["timestamp"]) - start).total_seconds() // 60)
            counts = buckets.setdefault(minute, [0, 0])
            counts[0] += 1
            counts[1] += row["temperature"] > 80
        if set(buckets) != set(range(minutes)) or any(
                counts != [rate, rate // 10] for counts in buckets.values()):
            failures.append("per_minute_rate_or_high_count")
        for row in generated:
            if row.get("scheduled_at"):
                delay = (_timestamp(row["timestamp"]) - _timestamp(row["scheduled_at"])).total_seconds()
                if delay < 0 or delay > 60 / rate:
                    failures.append("generator_pacing_drift")
    if latency_only:
        # These checks establish measurement coverage, not another full fault/replay suite.
        failures = [item for item in failures if item not in {
            "generated_missing_from_raw", "high_trigger_missing_from_raw",
            "raw_without_generated_record"}]
    evaluated.update({
        "runId": report.get("runId"), "rate": rate, "minutes": minutes,
        "latencyOnly": latency_only, "latencyMeasurementBasis": basis,
        "outputWriteTimeIncluded": basis == "independent_delta_visibility",
        "originalReadableOutputSlaProven": basis == "independent_delta_visibility" and not failures,
        "pass": not failures,
        "failures": sorted(set(failures)), "observedHigh": len(generated_high),
        "derivedPhysicalRows": len(derived),
        "derivedDistinctKeys": len(derived_keys),
        "derivedDuplicateDeliveries": len(derived) - len(derived_keys),
        "batches": result.get("batches") or [], "progress": result.get("progress") or [],
        "averageChecks": averages,
        "latencyRecords": latency_records,
        "rawPublication": raw_publication,
    })
    if latency_only:
        optimized = result.get("optimizations") or []
        if {row.get("table") for row in optimized} != {"lhspark.Events", "lhspark.Scores"}:
            evaluated["failures"].append("pre_run_optimize_missing")
            evaluated["pass"] = False
    return evaluated


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rate", type=int, default=30)
    parser.add_argument("--minutes", type=int, default=30)
    parser.add_argument("--latency-only", action="store_true")
    args = parser.parse_args()
    result = evaluate(json.loads(args.input.read_text(encoding="utf-8-sig")),
                      args.rate, args.minutes, args.latency_only)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({key: result[key] for key in
                      ("runId", "rate", "minutes", "pass", "failures", "latency_seconds")}))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
