import ast
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import types
import unittest
import urllib.error
import uuid


SOURCE = (Path(__file__).resolve().parents[1]
          / "fabric/notebooks/streaming_pipeline.py").read_text()


class GroupedProcessingTests(unittest.TestCase):
    def setUp(self):
        names = {"BatchWrites", "_process_record", "_score_record", "_quarantine",
                 "_write_failure_audit", "process_records", "replay_quarantined",
                 "_payload_digest", "_audit_payload_matches",
                 "_utcnow", "ApiRequestError", "KqlQueryError"}
        nodes = [node for node in ast.parse(SOURCE).body
                 if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
        self.saved = {name: {} for name in ("Events", "Scores", "Audit", "Quarantine")}
        self.order, self.effects, self.calls = [], {}, []
        test = self

        class AuditTable:
            def __init__(self, table):
                self.table = table

            def where(self, predicate):
                return self

            def limit(self, number):
                return self

            def collect(self):
                return [types.SimpleNamespace(asDict=lambda recursive, item=deepcopy(row): item)
                        for row in test.saved[self.table].values()]

        class Field:
            def isin(self, values):
                return self

            def __eq__(self, value):
                return self

            def __and__(self, other):
                return self

        field = Field()
        self.ns = {
            "datetime": datetime, "timezone": timezone, "hashlib": hashlib, "json": json,
            "math": math, "uuid": uuid, "urllib": __import__("urllib"),
            "spark": types.SimpleNamespace(table=lambda name: AuditTable(name)),
            "F": types.SimpleNamespace(col=lambda name: field),
            "RUN_ID": "explicit-replay-execution",
            **{f"TABLE_{name}": value for name, value in (
                ("EVENTS", "Events"), ("SCORES", "Scores"),
                ("AUDIT", "Audit"), ("QUARANTINE", "Quarantine"))},
        }
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "<grouped>", "exec"), self.ns)
        self.ns["_upsert"] = self.persist
        self.ns["_eventhouse_average"] = lambda event: (95.0, 1, datetime.now(), datetime.now())
        self.ns["_call_score_api"] = self.score
        self.ns["_audit_for"] = lambda run_id, message_id: self.saved["Audit"].get((run_id, message_id))

    def persist(self, table, rows):
        self.order.append(("persist", table))
        for row in rows:
            self.saved[table][(row["run_id"], row["message_id"])] = deepcopy(row)

    def score(self, event, calculated):
        key = (event["run_id"], event["message_id"])
        self.assertIn(key, self.saved["Events"])
        self.assertEqual(self.saved["Audit"][key]["status"], "EVENTS_WRITTEN")
        self.order.append(("api", key))
        self.calls.append(key)
        self.effects.setdefault(key, calculated)
        return 95, 1

    def row(self):
        event = {"run_id": str(uuid.uuid4()), "message_id": str(uuid.uuid4()),
                 "equipment_id": 41, "temperature": 95.0, "timestamp": "2026-11-03T22:00:00Z"}
        return {"topic": "topic", "partition": 0, "offset": 1, "timestamp": datetime.now(),
                "envelope_index": 0, "parsed": event,
                "value": json.dumps(event, sort_keys=True, separators=(",", ":"))}

    def test_grouped_merges_are_durable_before_per_event_api_and_replay_skips(self):
        rows = [self.row(), self.row()]
        self.ns["process_records"](deepcopy(rows))
        self.assertEqual(len(self.calls), 2)
        self.assertEqual([item for item in self.order if item[0] == "persist"],
                         [("persist", "Events"), ("persist", "Audit"),
                          ("persist", "Scores"), ("persist", "Audit")])
        self.ns["process_records"](deepcopy(rows))
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(len(self.saved["Scores"]), 2)

    def test_per_event_timestamps_bracket_enrichment_and_api_before_own_write(self):
        rows = [self.row(), self.row()]
        moments = []
        original_average, original_score, original_persist = (
            self.ns["_eventhouse_average"], self.ns["_call_score_api"], self.persist)
        def average(event):
            moments.append(("kql", event["message_id"], datetime.now(timezone.utc).replace(tzinfo=None)))
            return original_average(event)
        def score(event, calculated):
            moments.append(("api", event["message_id"], datetime.now(timezone.utc).replace(tzinfo=None)))
            return original_score(event, calculated)
        def persist(table, pending):
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            if table in ("Events", "Scores"):
                for row in pending:
                    prior = next(value[2] for value in moments
                                 if value[:2] == ("kql" if table == "Events" else "api", row["message_id"]))
                    self.assertLessEqual(prior, row["spark_processed_at"])
                    self.assertLessEqual(row["spark_processed_at"], now)
            original_persist(table, pending)
        self.ns["_eventhouse_average"], self.ns["_call_score_api"], self.ns["_upsert"] = average, score, persist
        self.ns["process_records"](rows)
        self.assertEqual(len(self.saved["Events"]), 2)
        self.assertEqual(len(self.saved["Scores"]), 2)

    def test_mixed_query_failure_and_api_failure_preserve_success_and_events(self):
        rows = [self.row() for _ in range(3)]
        query_failure = rows[0]["parsed"]["message_id"]
        api_failure = rows[1]["parsed"]["message_id"]
        original_score = self.score

        def average(event):
            if event["message_id"] == query_failure:
                raise self.ns["KqlQueryError"]("KQLHttp400")
            return 95.0, 1, datetime.now(), datetime.now()

        def score(event, calculated):
            if event["message_id"] == api_failure:
                raise self.ns["ApiRequestError"]("HTTP503", 2)
            return original_score(event, calculated)

        self.ns["_eventhouse_average"], self.ns["_call_score_api"] = average, score
        self.ns["process_records"](deepcopy(rows))
        self.assertEqual(len(self.saved["Events"]), 2)
        self.assertEqual(len(self.saved["Scores"]), 1)
        self.assertEqual({row["stage"] for row in self.saved["Quarantine"].values()}, {"KQL", "API"})
        self.assertIn("KQLHttp400", next(row["reason"] for row in
                                       self.saved["Quarantine"].values() if row["stage"] == "KQL"))
        self.assertIn("HTTP503", next(row["reason"] for row in
                                    self.saved["Quarantine"].values() if row["stage"] == "API"))

    def test_successful_empty_average_scores_trigger_and_retains_missing_visibility(self):
        row = self.row()
        self.ns["_eventhouse_average"] = lambda event: (None, 0, None, datetime.now())
        self.ns["process_records"]([row])
        event = next(iter(self.saved["Events"].values()))
        audit = next(iter(self.saved["Audit"].values()))
        self.assertEqual(event["calculated_temperature"], row["parsed"]["temperature"])
        self.assertEqual(audit["fallback_reason"], "SuccessfulEmptyOrNullKqlResult")
        self.assertIsNone(audit["raw_visible_at"])
        self.assertEqual(audit["query_unique_count"], 0)
        self.assertEqual(len(self.saved["Scores"]), 1)
        self.assertFalse(self.saved["Quarantine"])

    def test_explicit_quarantine_replay_marks_resolution_and_preserves_original_key(self):
        row = self.row()
        def unavailable(event, calculated):
            raise self.ns["ApiRequestError"]("HTTP503", 2)
        self.ns["_call_score_api"] = unavailable
        self.ns["process_records"]([deepcopy(row)])
        key = (row["parsed"]["run_id"], row["parsed"]["message_id"])
        original_time = self.saved["Quarantine"][key]["quarantined_at"]
        self.ns["_call_score_api"] = self.score
        self.ns["_eventhouse_average"] = lambda event: self.fail("Replay must retain calculated snapshot")
        result = self.ns["replay_quarantined"](*key)
        self.assertEqual(result["status"], "SCORED")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.saved["Quarantine"][key]["quarantined_at"], original_time)
        resolution = json.loads(self.saved["Quarantine"][key]["resolution"])
        self.assertEqual(resolution["status"], "RESOLVED")
        self.assertEqual(resolution["execution_run_id"], "explicit-replay-execution")
        self.assertEqual(self.saved["Quarantine"][key]["replay_of"], ":".join(key))
        self.ns["replay_quarantined"](*key)
        self.assertEqual(len(self.calls), 1)

    def test_resolution_write_failure_retries_without_another_api_call(self):
        row = self.row()
        def unavailable(event, calculated):
            raise self.ns["ApiRequestError"]("HTTP503", 2)
        self.ns["_call_score_api"] = unavailable
        self.ns["process_records"]([deepcopy(row)])
        key = (row["parsed"]["run_id"], row["parsed"]["message_id"])
        self.ns["_call_score_api"] = self.score
        def persist_except_resolution(table, rows):
            if table == "Quarantine":
                raise RuntimeError("ResolutionWriteFailure")
            return self.persist(table, rows)
        self.ns["_upsert"] = persist_except_resolution
        with self.assertRaisesRegex(RuntimeError, "ResolutionWriteFailure"):
            self.ns["replay_quarantined"](*key)
        self.ns["_upsert"] = self.persist
        self.ns["replay_quarantined"](*key)
        self.assertEqual(len(self.calls), 1)

    def test_quarantine_replay_failure_remains_explicitly_unresolved(self):
        row = self.row()
        def unavailable(event, calculated):
            raise self.ns["ApiRequestError"]("HTTP503", 2)
        self.ns["_call_score_api"] = unavailable
        self.ns["process_records"]([deepcopy(row)])
        key = (row["parsed"]["run_id"], row["parsed"]["message_id"])
        with self.assertRaisesRegex(RuntimeError, "QuarantineReplayDidNotScore"):
            self.ns["replay_quarantined"](*key)
        self.assertIsNone(self.saved["Quarantine"][key]["resolution"])

    def test_crash_after_api_before_score_reuses_durable_idempotency(self):
        rows = [self.row()]
        def fail_scores(table, pending):
            if table == "Scores":
                raise RuntimeError("StorageFailure")
            self.persist(table, pending)
        self.ns["_upsert"] = fail_scores
        with self.assertRaisesRegex(RuntimeError, "StorageFailure"):
            self.ns["process_records"](deepcopy(rows))
        self.assertEqual(len(self.saved["Events"]), 1)
        self.assertEqual(len(self.effects), 1)
        self.ns["_upsert"] = self.persist
        self.ns["_eventhouse_average"] = lambda event: self.fail("Persisted KQL snapshot must be reused")
        self.ns["process_records"](deepcopy(rows))
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(len(self.effects), 1)
        self.assertEqual(len(self.saved["Scores"]), 1)

    def test_failure_before_pre_api_audit_never_calls_api(self):
        def fail_audit(table, pending):
            if table == "Audit":
                raise RuntimeError("AuditFailure")
            self.persist(table, pending)
        self.ns["_upsert"] = fail_audit
        with self.assertRaisesRegex(RuntimeError, "AuditFailure"):
            self.ns["process_records"]([self.row()])
        self.assertFalse(self.calls)
        self.assertEqual(len(self.saved["Events"]), 1)

    def test_restart_after_scores_before_final_audit_keeps_one_logical_score(self):
        rows = [self.row()]
        audit_writes = 0
        def fail_final_audit(table, pending):
            nonlocal audit_writes
            if table == "Audit":
                audit_writes += 1
                if audit_writes == 2:
                    raise RuntimeError("FinalAuditFailure")
            self.persist(table, pending)
        self.ns["_upsert"] = fail_final_audit
        with self.assertRaisesRegex(RuntimeError, "FinalAuditFailure"):
            self.ns["process_records"](deepcopy(rows))
        self.assertEqual(len(self.saved["Scores"]), 1)
        self.ns["_upsert"] = self.persist
        self.ns["process_records"](deepcopy(rows))
        self.assertEqual(len(self.saved["Scores"]), 1)
        self.assertEqual(len(self.effects), 1)
        self.assertEqual(next(iter(self.saved["Audit"].values()))["status"], "SCORED")

    def test_malformed_record_does_not_block_other_events(self):
        malformed = self.row()
        malformed["parsed"] = None
        malformed["value"] = "{invalid"
        self.ns["process_records"]([malformed, self.row()])
        self.assertEqual(len(self.saved["Scores"]), 1)
        self.assertEqual(next(iter(self.saved["Quarantine"].values()))["stage"], "VALIDATION")

    def test_duplicate_envelope_scores_once_and_conflict_is_quarantined_without_overwrite(self):
        row = self.row()
        self.ns["process_records"]([deepcopy(row), deepcopy(row)])
        self.assertEqual(len(self.calls), 1)
        conflict = deepcopy(row)
        conflict["parsed"]["temperature"] = 96.0
        conflict["value"] = json.dumps(conflict["parsed"], sort_keys=True, separators=(",", ":"))
        self.ns["process_records"]([conflict])
        self.ns["process_records"]([deepcopy(row), conflict])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(next(iter(self.saved["Quarantine"].values()))["stage"], "IDENTITY")
        self.assertEqual(next(iter(self.saved["Audit"].values()))["status"], "SCORED")

    def test_transport_metadata_changes_and_json_whitespace_do_not_rescore_logical_event(self):
        row = self.row()
        row["parsed"].update(EventEnqueuedUtcTime="first", EventProcessedUtcTime="first", PartitionId=0)
        row["value"] = json.dumps(row["parsed"])
        repeat = deepcopy(row)
        repeat["parsed"].update(EventEnqueuedUtcTime="later", EventProcessedUtcTime="later", PartitionId=3)
        repeat["value"] = json.dumps(repeat["parsed"], indent=2)
        self.ns["process_records"]([deepcopy(row), deepcopy(repeat)])
        self.ns["process_records"]([deepcopy(repeat)])
        self.assertEqual(len(self.calls), 1)
        self.assertFalse(self.saved["Quarantine"])
        key = (row["parsed"]["run_id"], row["parsed"]["message_id"])
        # A retained legacy raw-JSON hash remains readable without rewriting history.
        self.saved["Audit"][key]["payload_hash"] = hashlib.sha256(
            self.saved["Audit"][key]["payload_json"].encode()).hexdigest()
        self.ns["process_records"]([deepcopy(repeat)])
        self.assertEqual(len(self.calls), 1)

    def test_other_payload_fields_still_conflict_and_corrupt_audit_fails_closed(self):
        row = self.row()
        self.ns["process_records"]([deepcopy(row)])
        repeat = deepcopy(row)
        repeat["parsed"]["user_field"] = "changed"
        repeat["value"] = json.dumps(repeat["parsed"])
        self.ns["process_records"]([repeat])
        self.assertEqual(next(iter(self.saved["Quarantine"].values()))["stage"], "IDENTITY")
        key = (row["parsed"]["run_id"], row["parsed"]["message_id"])
        self.saved["Audit"][key]["payload_hash"] = "corrupt"
        with self.assertRaisesRegex(RuntimeError, "RetainedAuditPayloadHashMismatch"):
            self.ns["process_records"]([deepcopy(row)])


if __name__ == "__main__":
    unittest.main()
