import copy
from datetime import datetime, timedelta, timezone
import unittest

from src.reconciliation.evaluate_run import evaluate


class RunEvaluationTests(unittest.TestCase):
    def setUp(self):
        start = datetime(2026, 11, 3, tzinfo=timezone.utc)
        generated = [{
            "run_id": "run", "message_id": str(index), "equipment_id": index,
            "temperature": 95 if index < 3 else 20,
            "timestamp": (start + timedelta(seconds=index * 2)).isoformat(),
            "status": "generated",
        } for index in range(30)]
        high = generated[:3]
        events = [{
            "run_id": "run", "message_id": row["message_id"],
            "equipment_id": row["equipment_id"], "message_timestamp": row["timestamp"],
            "calculated_temperature": 95, "table": "Events",
            "independently_read_at": (
                datetime.fromisoformat(row["timestamp"]) + timedelta(seconds=19)).isoformat(),
        } for row in high]
        scores = [dict(row, table="Scores", score=95) for row in events]
        self.report = {
            "runId": "run", "generatorExitCode": 0, "jobStatus": "Completed",
            "generatorManifest": generated + [dict(row, status="acknowledged") for row in generated],
            "independentObserverExitCode": 0, "independentVisibility": events + scores,
            "result": {
                "error": None, "rawRecords": generated, "rawWindowRecords": generated,
                "derivedRecords": [{"event": dict(row)} for row in high],
                "outputRecords": {
                    "lhspark.Events": events, "lhspark.Scores": scores,
                    "lhspark.ProcessingAudit": [
                        dict(row, status="SCORED", query_unique_count=1,
                             raw_visible_at=row["timestamp"], fallback_reason=None,
                             api_attempts=1, api_result="95") for row in high],
                    "lhspark.Quarantine": [],
                },
                "optimizations": [{"table": "lhspark.Events"}, {"table": "lhspark.Scores"}],
                "stageTimings": [{
                    "parameters": {"runId": row["run_id"], "messageId": row["message_id"],
                                   "equipmentId": row["equipment_id"], "triggerTime": row["timestamp"]},
                    "aggregateProjection": ["average", "unique_count", "trigger_visible", "window_records"],
                    "aggregateRows": [[95, 1, 1, [{
                        key: row[key] for key in ("run_id", "message_id", "temperature", "timestamp")}]]],
                } for row in high] + [{
                    "stage": "score_api", "runId": row["run_id"], "messageId": row["message_id"],
                    "attempts": 1, "elapsedSeconds": 0.3,
                } for row in high],
            },
        }

    def test_complete_run_passes(self):
        result = evaluate(self.report, 30, 1)
        self.assertTrue(result["pass"])
        self.assertEqual(len(result["latencyRecords"]), 6)

    def test_spark_prewrite_basis_uses_persisted_per_event_timestamp_without_reader(self):
        self.report["latencyMeasurementBasis"] = "spark_pre_write"
        self.report.pop("independentVisibility")
        self.report.pop("independentObserverExitCode")
        for table, seconds in (("Events", 20), ("Scores", 30)):
            for row in self.report["result"]["outputRecords"][f"lhspark.{table}"]:
                row["spark_processed_at"] = (
                    datetime.fromisoformat(row["message_timestamp"])
                    + timedelta(seconds=seconds)).replace(tzinfo=None).isoformat()
        result = evaluate(self.report, 30, 1)
        self.assertTrue(result["pass"], result["failures"])
        self.assertEqual(result["latencyMeasurementBasis"], "spark_pre_write")
        self.assertFalse(result["outputWriteTimeIncluded"])
        self.assertFalse(result["originalReadableOutputSlaProven"])
        self.assertEqual({row["latencySeconds"] for row in result["latencyRecords"]}, {20, 30})
        row = self.report["result"]["outputRecords"]["lhspark.Scores"][0]
        row["spark_processed_at"] = (
            datetime.fromisoformat(row["message_timestamp"]) + timedelta(seconds=30.001)).isoformat()
        self.assertIn("scores_latency_exceeded", evaluate(self.report, 30, 1)["failures"])
        row["spark_processed_at"] = (
            datetime.fromisoformat(row["message_timestamp"]) - timedelta(seconds=1)).isoformat()
        self.assertFalse(evaluate(self.report, 30, 1)["pass"])
        row["spark_processed_at"] = None
        self.assertIn("spark_processing_timestamp_missing", evaluate(self.report, 30, 1)["failures"])

    def test_spark_basis_cannot_reuse_old_reader_timestamps_or_unknown_basis(self):
        self.report["latencyMeasurementBasis"] = "spark_pre_write"
        result = evaluate(self.report, 30, 1)
        self.assertFalse(result["pass"])
        self.assertIn("spark_processing_timestamp_missing", result["failures"])
        self.report["latencyMeasurementBasis"] = "written_at"
        with self.assertRaises(ValueError):
            evaluate(self.report, 30, 1)

    def test_raw_publication_has_separate_five_minute_limit_and_exact_coverage(self):
        self.report["rawPublicationMeasured"] = True
        self.report["independentRawObserverExitCode"] = 0
        self.report["independentRawVisibility"] = [
            dict(row, table="RawEvents", independently_read_at=(
                datetime.fromisoformat(row["timestamp"]) + timedelta(seconds=300)).isoformat())
            for row in self.report["result"]["rawRecords"]]
        result = evaluate(self.report, 30, 1)
        self.assertTrue(result["pass"])
        self.assertEqual(result["rawPublication"]["maximumSeconds"], 300)
        row = self.report["independentRawVisibility"][0]
        row["independently_read_at"] = (
            datetime.fromisoformat(row["timestamp"]) + timedelta(seconds=301)).isoformat()
        self.assertIn("raw_publication_latency_exceeded", evaluate(self.report, 30, 1)["failures"])
        self.report["independentRawVisibility"].pop()
        self.assertIn("raw_publication_coverage_or_reader_failure",
                      evaluate(self.report, 30, 1)["failures"])

    def test_spark_naive_utc_output_matches_offset_aware_independent_read(self):
        outputs = self.report["result"]["outputRecords"]
        for table in ("Events", "Scores"):
            outputs[f"lhspark.{table}"] = copy.deepcopy(outputs[f"lhspark.{table}"])
            for row in outputs[f"lhspark.{table}"]:
                row["message_timestamp"] = row["message_timestamp"].replace("+00:00", "")
        self.assertTrue(evaluate(self.report, 30, 1)["pass"])

    def test_average_checks_include_prior_runs_for_same_equipment_and_event_time(self):
        earlier = dict(self.report["result"]["rawRecords"][0], run_id="prior", temperature=20,
                       timestamp="2026-11-02T23:59:00+00:00")
        self.report["result"]["rawWindowRecords"] = (
            self.report["result"]["rawWindowRecords"] + [earlier])
        aggregate = self.report["result"]["stageTimings"][0]["aggregateRows"][0]
        aggregate[0], aggregate[1] = 57.5, 2
        aggregate[3].append({key: earlier[key] for key in
                             ("run_id", "message_id", "temperature", "timestamp")})
        self.report["result"]["outputRecords"]["lhspark.ProcessingAudit"][0]["query_unique_count"] = 2
        self.assertIn("calculated_average_mismatch", evaluate(self.report, 30, 1)["failures"])
        self.report["result"]["outputRecords"]["lhspark.Events"][0]["calculated_temperature"] = 57.5
        self.assertTrue(evaluate(self.report, 30, 1)["pass"])

    def test_valid_empty_snapshot_can_fallback_despite_later_final_raw_arrival(self):
        self.report["result"]["stageTimings"][0]["aggregateRows"] = [[None, 0, 0, []]]
        audit = self.report["result"]["outputRecords"]["lhspark.ProcessingAudit"][0]
        audit.update(query_unique_count=0, raw_visible_at=None,
                     fallback_reason="SuccessfulEmptyOrNullKqlResult")
        result = evaluate(self.report, 30, 1)
        self.assertTrue(result["pass"])
        self.assertFalse(result["averageChecks"][0]["triggerVisibleAtQuery"])

    def test_duplicate_raw_and_changed_derived_payload_fail(self):
        self.report["result"]["rawRecords"] = self.report["result"]["rawRecords"] + [
            self.report["result"]["rawRecords"][0]]
        self.report["result"]["derivedRecords"][0]["event"]["temperature"] = 96
        result = evaluate(self.report, 30, 1)
        self.assertEqual(result["physical_rows"]["raw"], 31)
        self.assertEqual(result["duplicate_keys"]["raw"], ["run:0"])
        self.assertIn("derived_payload_mismatch", result["failures"])

    def test_identical_delivery_duplicates_are_reported_separately_from_logical_outputs(self):
        self.report["result"]["rawRecords"] = self.report["result"]["rawRecords"] + [
            self.report["result"]["rawRecords"][0]]
        self.report["result"]["derivedRecords"].append(
            copy.deepcopy(self.report["result"]["derivedRecords"][0]))
        result = evaluate(self.report, 30, 1)
        self.assertTrue(result["pass"])
        self.assertEqual(result["derivedDuplicateDeliveries"], 1)
        self.assertEqual(result["derivedDistinctKeys"], 3)

    def test_one_slow_record_fails_even_with_complete_counts(self):
        row = self.report["independentVisibility"][0]
        row["independently_read_at"] = (
            datetime.fromisoformat(row["message_timestamp"]) + timedelta(seconds=21)).isoformat()
        self.assertIn("events_latency_exceeded", evaluate(self.report, 30, 1)["failures"])

    def test_observer_failure_invalidates_first_read_times(self):
        self.report["independentObserverExitCode"] = 1
        self.assertFalse(evaluate(self.report, 30, 1)["pass"])

    def test_load_requires_both_optimizations_and_complete_visibility(self):
        self.assertTrue(evaluate(self.report, 30, 1, True)["pass"])
        self.report["result"]["optimizations"].pop()
        self.assertIn("pre_run_optimize_missing", evaluate(self.report, 30, 1, True)["failures"])

    def test_physical_duplicates_and_different_derived_ids_fail(self):
        report = copy.deepcopy(self.report)
        report["result"]["outputRecords"]["lhspark.Scores"].append(
            report["result"]["outputRecords"]["lhspark.Scores"][0])
        report["result"]["derivedRecords"][0]["event"]["message_id"] = "unexpected"
        failures = evaluate(report, 30, 1)["failures"]
        self.assertIn("scores_physical_duplicates", failures)
        self.assertIn("derived_key_mismatch", failures)


if __name__ == "__main__":
    unittest.main()
