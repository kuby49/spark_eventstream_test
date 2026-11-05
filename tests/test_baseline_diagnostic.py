import ast
import json
from pathlib import Path
import types
import unittest


ROOT = Path(__file__).resolve().parents[1]


class BaselineDiagnosticTests(unittest.TestCase):
    def test_spark4_progress_json_normalizes_nested_source_offsets_before_readiness(self):
        baseline = (ROOT / "fabric/notebooks/baseline_diagnostic_tail.py").read_text()
        node = next(node for node in ast.parse(baseline).body
                    if isinstance(node, ast.FunctionDef) and node.name == "baseline_progress_dict")
        namespace = {"json": json}
        exec(compile(ast.Module(body=[node], type_ignores=[]), "<progress>", "exec"), namespace)
        expected = {"sources": [{"endOffset": {"topic": {"0": 49}}}], "batchId": 0}
        spark4 = types.SimpleNamespace(json=json.dumps(expected))
        self.assertEqual(namespace["baseline_progress_dict"](spark4), expected)
        self.assertEqual(namespace["baseline_progress_dict"](expected), expected)
        self.assertIsNone(namespace["baseline_progress_dict"](None))
        self.assertIn("progress = baseline_progress_dict(active.lastProgress)", baseline)

    def test_baseline_combines_production_helpers_with_bounded_consumer(self):
        source = (ROOT / "fabric/notebooks/streaming_pipeline.py").read_text()
        helpers = (ROOT / "fabric/notebooks/functional_processing_diagnostic_tail.py").read_text()
        baseline = (ROOT / "fabric/notebooks/baseline_diagnostic_tail.py").read_text()
        combined = (source.split('if RUN_MODE == "private-canary":')[0]
                    + helpers.split("gate_output_records = {}")[0] + baseline)
        compile(combined, "baseline", "exec")
        self.assertIn("process_records(records)", baseline)
        self.assertIn('"startingOffsets", json.dumps({KAFKA_TOPIC: warm_starts})', baseline)
        self.assertIn('max(item["beginning"], item["end"] - 1)', baseline)
        self.assertIn("if not armed and caught_up:", baseline)
        self.assertIn('"newEventsSent": 0', baseline)
        self.assertIn("time.monotonic() + TEST_MINUTES * 60 + 600", baseline)
        self.assertIn("active.stop()", baseline)
        self.assertIn('"acceptancePassed": False', baseline)
        self.assertIn("baseline_progress_keys", baseline)
        self.assertIn("completed_events == len(baseline_derived)", baseline)
        self.assertIn('int(progress["batchId"]) >= baseline_batches[-1]["batchId"]', baseline)
        self.assertIn('"SCORED", "API_FAILED", "QUARANTINED"', baseline)

    def test_baseline_uses_exact_approved_rate_and_no_shutdown(self):
        runner = (ROOT / "scripts/Invoke-EventHubsAmqpDiagnostic.ps1").read_text()
        runtime = (ROOT / "scripts/Invoke-EventHubsAmqpPrivateRuntime.ps1").read_text()
        self.assertIn("Baseline requires functional-processing, KeepRunning", runner)
        self.assertIn("--rate $TestRate --minutes $TestMinutes --run-id $RunId", runtime)
        self.assertIn("$report.generatorManifest", runtime)
        self.assertIn("$report.latencyMeasurementBasis='spark_pre_write'", runtime)
        self.assertNotIn("$report.independentVisibility", runtime)
        self.assertNotIn("$report.independentObserverArmed=", runtime)
        self.assertLess(runtime.index("$report.independentRawObserverArmed="),
                        runtime.index("$producerArguments="))
        self.assertIn("$report.independentRawObserverExitCode=", runtime)
        self.assertIn("$measureRawPublication=$Baseline -and -not $LoadTest -and -not $SustainedDiagnostic",
                      runner)

    def test_load_optimize_finishes_before_consumer_readiness(self):
        baseline = (ROOT / "fabric/notebooks/baseline_diagnostic_tail.py").read_text()
        self.assertIn('spark.sql(f"OPTIMIZE {table}").collect()', baseline)
        self.assertLess(baseline.index('spark.sql(f"OPTIMIZE {table}")'),
                        baseline.index('functional-ready-'))
        self.assertIn("for table in (TABLE_EVENTS, TABLE_SCORES)", baseline)
        runner = (ROOT / "scripts/Invoke-EventHubsAmqpDiagnostic.ps1").read_text()
        self.assertIn("$testMinutes=if($SustainedDiagnostic){5}elseif($LoadTest){15}else{30}", runner)
        self.assertIn("Initial 30-minute baseline must pass", runner)


if __name__ == "__main__":
    unittest.main()
