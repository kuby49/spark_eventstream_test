import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from src.reconciliation.reconcile import reconcile


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.key = {"run_id": "run-1", "message_id": "message-1"}
        self.generated = [{
            **self.key,
            "equipment_id": 7,
            "temperature": 81.0,
            "timestamp": "2026-11-02T12:00:00Z",
        }]
        self.acknowledged = [self.key]
        self.raw = [{
            **self.key,
            "equipment_id": 7,
            "temperature": 81.0,
            "timestamp": "2026-11-02T12:00:00Z",
        }]
        self.events = [{
            **self.key,
            "equipment_id": 7,
            "message_timestamp": "2026-11-02T12:00:00Z",
            "independently_read_at": "2026-11-02T12:00:10Z",
        }]
        self.scores = [{
            **self.key,
            "score": 81,
            "independently_read_at": "2026-11-02T12:00:20Z",
        }]

    def test_scored_event_reconciles_with_independent_latency(self):
        result = reconcile(
            self.generated,
            self.acknowledged,
            self.raw,
            self.events,
            self.scores,
            [],
            expected_generated=1,
            expected_high=1,
        )
        self.assertTrue(result["pass"], result["failures"])
        self.assertEqual(result["latency_seconds"]["events"]["max"], 10)
        self.assertEqual(result["latency_seconds"]["scores"]["max"], 20)

    def test_latency_uses_each_row_not_only_percentiles(self):
        self.scores[0]["independently_read_at"] = "2026-11-02T12:00:31Z"
        result = reconcile(
            self.generated, self.acknowledged, self.raw, self.events, self.scores, [],
            expected_generated=1, expected_high=1,
        )
        self.assertIn("scores_latency_exceeded", result["failures"])

    def test_api_failure_is_a_resolved_event_outcome(self):
        result = reconcile(
            self.generated, self.acknowledged, self.raw, self.events, [],
            [{**self.key, "stage": "API"}],
            expected_generated=1, expected_high=1,
            require_full_scoring=False,
        )
        self.assertTrue(result["pass"], result["failures"])
        self.assertEqual(result["outcomes"]["api_failed_with_event"], ["run-1:message-1"])

    def test_unknown_high_temperature_event_is_unresolved(self):
        result = reconcile(
            self.generated, self.acknowledged, self.raw, [], [], [],
            expected_generated=1, expected_high=1,
        )
        self.assertIn("unresolved_or_inconsistent_outcomes", result["failures"])

    def test_missing_acknowledgement_and_raw_event_fail_reconciliation(self):
        result = reconcile(
            self.generated, [], [], self.events, self.scores, [],
            expected_generated=1, expected_high=1,
        )
        self.assertIn("generated_without_send_acknowledgement", result["failures"])
        self.assertIn("generated_missing_from_raw", result["failures"])

    def test_baseline_requires_a_score_for_every_high_trigger(self):
        result = reconcile(
            self.generated, self.acknowledged, self.raw, self.events, [],
            [{**self.key, "stage": "API"}],
            expected_generated=1, expected_high=1,
        )
        self.assertIn("baseline_score_set_mismatch", result["failures"])

    def test_manifest_classifies_generation_ack_and_attempt_failures(self):
        from src.reconciliation.reconcile import _load_generator_manifest

        with TemporaryDirectory() as folder:
            path = Path(folder) / "manifest.jsonl"
            records = [
                {**self.generated[0], "status": "generated"},
                {**self.key, "status": "failed", "attempt": 1},
                {**self.key, "status": "acknowledged", "attempts": 2},
            ]
            path.write_text(
                "".join(json.dumps(record) + "\n" for record in records),
                encoding="utf-8",
            )
            generated, acknowledged, failed = _load_generator_manifest(path)
        self.assertEqual(len(generated), 1)
        self.assertEqual(len(acknowledged), 1)
        self.assertEqual(len(failed), 1)


if __name__ == "__main__":
    unittest.main()
