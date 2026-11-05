import ast
from datetime import datetime, timezone
import json
from pathlib import Path
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]


class StressDiagnosticTests(unittest.TestCase):
    def test_stress_exit_keeps_durable_full_evidence_and_orders_phase_markers(self):
        source = (ROOT / "fabric/notebooks/baseline_diagnostic_tail.py").read_text()
        start = source.index("baseline_json = json.dumps(baseline_report")
        for stress, fail_write in ((True, False), (True, True), (False, False)):
            calls = []
            report = {"runId": "run", "payload": "x" * 20000}

            class NotebookExit(BaseException):
                pass

            def put(path, value, overwrite):
                calls.append(("put", path, json.loads(value)))
                if fail_write and path.endswith("functional-processing-run.json"):
                    raise OSError("storage failure")

            def exit_notebook(value):
                calls.append(("exit", json.loads(value)))
                raise NotebookExit()

            namespace = {
                "baseline_report": report, "baseline_encode": str, "json": json,
                "RUN_ID": "run", "STRESS_TEST": stress,
                "datetime": datetime, "timezone": timezone,
                "notebookutils": types.SimpleNamespace(
                    fs=types.SimpleNamespace(put=put),
                    notebook=types.SimpleNamespace(exit=exit_notebook)),
            }
            with self.assertRaises(OSError if fail_write else NotebookExit):
                exec(compile(source[start:], "<reporting>", "exec"), namespace)
            if fail_write:
                self.assertFalse(any(call[0] == "exit" for call in calls))
                self.assertEqual(len(calls[0][2]["phases"]), 1)
            elif stress:
                self.assertEqual(calls[1][2], report)
                self.assertEqual([phase["phase"] for phase in calls[-2][2]["phases"]],
                                 ["before_final_evidence_write",
                                  "after_final_evidence_write", "before_notebook_exit"])
                self.assertEqual(calls[-1][1]["kind"], "persisted_stress_evidence_v1")
                self.assertEqual(calls[-1][1]["runId"], "run")
                self.assertLess(len(json.dumps(calls[-1][1])), 512)
            else:
                self.assertEqual(calls[-1], ("exit", report))
        runtime = (ROOT / "scripts/Invoke-EventHubsAmqpPrivateRuntime.ps1").read_text()
        self.assertIn("Stress evidence exit pointer identity mismatch", runtime)
        self.assertIn("Persisted stress evidence identity mismatch", runtime)

    def test_combined_notebook_compiles_and_reports_health_without_changing_processing(self):
        source = (ROOT / "fabric/notebooks/streaming_pipeline.py").read_text()
        helpers = (ROOT / "fabric/notebooks/functional_processing_diagnostic_tail.py").read_text()
        tail = (ROOT / "fabric/notebooks/baseline_diagnostic_tail.py").read_text()
        combined = (source.split('if RUN_MODE == "private-canary":')[0]
                    + helpers.split("gate_output_records = {}")[0] + tail)
        compile(combined, "stress-notebook", "exec")
        self.assertIn("process_records(records)", tail)
        self.assertIn('"completedLogicalEvents": sum(', tail)
        self.assertIn('"lagEnvelopes":', tail)
        self.assertNotIn("uiWebUrl", tail)
        runtime = (ROOT / "scripts/Invoke-EventHubsAmqpPrivateRuntime.ps1").read_text()
        self.assertIn("@('resourceUsage','executors','stages')", runtime)
        self.assertIn("Where-Object jobInstanceId -eq $report.jobId", runtime)
        self.assertIn("active.stop()", tail)

    def test_stress_mode_is_explicit_and_does_not_forge_baseline_acceptance(self):
        runner = (ROOT / "scripts/Invoke-EventHubsAmqpDiagnostic.ps1").read_text()
        self.assertIn("$LoadTest -and -not $StressTest", runner)
        self.assertIn("stress-baseline-handoff.json", runner)
        self.assertIn("$handoff.counts.acknowledged -ne 900", runner)
        self.assertIn("-StressTest `$$($StressTest.IsPresent", runner)
        self.assertIn("Stress requires already-active F8", runner)
        self.assertIn("Stress requires already-running VM", runner)
        self.assertIn("    }\n    if($StressTest){\n", runner)

    def test_monitor_covers_owned_producer_until_exit_and_cancels_only_owned_job(self):
        runtime = (ROOT / "scripts/Invoke-EventHubsAmqpPrivateRuntime.ps1").read_text()
        for reason in ("ConsumerTerminatedWhileSending", "HealthTelemetryStale",
                       "ProducerDeliveryFailure", "ExplicitRejectionOrThrottling",
                       "NoCommittedProgressWithBacklog", "SustainedUnsafeBacklog",
                       "ProducerExitedUnsuccessfully"):
            self.assertIn(reason, runtime)
        self.assertIn("while(-not $process.WaitForExit(10000))", runtime)
        self.assertIn("Stop-Process -Id $process.Id", runtime)
        self.assertIn('Request Post "$jobUri/cancel"', runtime)
        self.assertIn("if(-not $StressTest -and -not $MaintenanceOnly){\n            $productionDefinition", runtime)
        self.assertIn("if($Baseline -or $MaintenanceOnly){", runtime)
        self.assertIn("stress-full-$RunId.json", runtime)
        self.assertNotIn("'suspend'", runtime)
        self.assertNotIn("'deallocate'", runtime)

    def test_steps_are_bounded_and_continue_on_latency_only_failure(self):
        wrapper = (ROOT / "scripts/Invoke-SteppedStressTests.ps1").read_text()
        self.assertIn("$step -le $MaximumSteps", wrapper)
        self.assertIn("AddMinutes($MaximumElapsedMinutes)", wrapper)
        self.assertIn("if($result.sustained)", wrapper)
        self.assertNotIn("if($result.latencyBoundary)", wrapper)
        self.assertIn("-KeepRunning -Baseline -LoadTest -StressTest", wrapper)
        self.assertIn("$previous.allocationBasis -eq $summary.allocationBasis", wrapper)
        self.assertIn("historicalOtherAllocationRuns", wrapper)
        self.assertIn("Stress allocation envelope missing or changed", wrapper)

    def test_scaling_records_only_observed_transitions_and_preserves_sample_history(self):
        source = (ROOT / "fabric/notebooks/baseline_diagnostic_tail.py").read_text()
        node = next(node for node in ast.parse(source).body
                    if isinstance(node, ast.FunctionDef) and node.name == "stress_sample")
        counts = iter((1, 3, 2))
        clock = iter(range(100, 1000, 20))
        def allocation():
            count = next(counts)
            return {"activeExecutors": count, "checkedAtUtc": f"sample-{count}",
                    "observedDriverAndExecutorCores": 8 + count * 8}
        publications = []
        namespace = {
            "STRESS_TEST": True, "time": types.SimpleNamespace(monotonic=lambda: next(clock)),
            "stress_last_sample": 0, "stress_last_offsets": 0, "stress_broker": None,
            "stress_previous_executors": None, "stress_scaling_events": [],
            "_verify_session_allocation": allocation,
            "broker_metadata": lambda: {"partitionOffsets": []},
            "KAFKA_TOPIC": "topic", "RUN_ID": "run", "datetime": datetime, "timezone": timezone,
            "active": types.SimpleNamespace(isActive=True), "baseline_batches": [],
            "baseline_derived": [], "stage_timings": [], "stress_health": [], "json": json,
            "notebookutils": types.SimpleNamespace(fs=types.SimpleNamespace(
                put=lambda path, value, overwrite: publications.append(json.loads(value)))),
        }
        exec(compile(ast.Module(body=[node], type_ignores=[]), "<scaling>", "exec"), namespace)
        for _ in range(3):
            namespace["stress_sample"](None)
        events = namespace["stress_scaling_events"]
        self.assertEqual([(event["fromExecutors"], event["toExecutors"]) for event in events],
                         [(1, 3), (3, 2)])
        self.assertEqual([event["direction"] for event in events], ["up", "down"])
        self.assertEqual([event["observedDriverAndExecutorCores"] for event in events], [32, 24])
        self.assertEqual(len(namespace["stress_health"][0]["scalingEvents"]), 0)
        self.assertEqual(len(publications[-1]["scalingEvents"]), 2)

    def test_surgical_deployment_sets_ipynb_format_and_verifies_cell_sources(self):
        script = (ROOT / "scripts/Set-NotebookDynamicAllocationPrivateRuntime.ps1").read_text()
        self.assertIn("-NotePropertyName format -NotePropertyValue 'ipynb' -Force", script)
        self.assertIn("Existing node shape differs", script)
        self.assertIn("Updated definition source readback differs", script)
        self.assertIn("if($replacements -ne 1)", script)
        self.assertIn("'x-ms-fabric-skill'='spark-cli'", script)

    def test_post_test_optimize_requires_stopped_consumer_and_reports_maintenance_errors(self):
        source = (ROOT / "fabric/notebooks/baseline_diagnostic_tail.py").read_text()
        self.assertIn("if LOAD_TEST and not STRESS_TEST:", source)
        node = next(node for node in ast.parse(source).body
                    if isinstance(node, ast.If) and isinstance(node.test, ast.Name)
                    and node.test.id == "STRESS_TEST")
        self.assertLess(source.index("active.stop()"), source.index("if STRESS_TEST:"))
        for active_consumer, processing_error in ((False, None), (True, None),
                                                  (False, {"errorType": "ProcessingFailed"})):
            calls = []
            namespace = {
                "STRESS_TEST": True, "baseline_error": processing_error,
                "post_step_optimization": {"status": "NotRequested", "results": []},
                "datetime": datetime, "timezone": timezone,
                "active": types.SimpleNamespace(isActive=active_consumer),
                "TABLE_EVENTS": "lhspark.Events", "TABLE_SCORES": "lhspark.Scores",
                "TABLE_AUDIT": "lhspark.ProcessingAudit",
                "TABLE_QUARANTINE": "lhspark.Quarantine",
                "spark": types.SimpleNamespace(sql=lambda statement: (
                    calls.append(statement) or types.SimpleNamespace(collect=lambda: []))),
                "_verify_session_allocation": lambda: {"activeExecutors": 2},
                "traceback": __import__("traceback"),
            }
            exec(compile(ast.Module(body=[node], type_ignores=[]), "<post-optimize>", "exec"),
                 namespace)
            report = namespace["post_step_optimization"]
            if processing_error:
                self.assertEqual(report["status"], "SkippedAfterProcessingFailure")
                self.assertEqual(calls, [])
            elif active_consumer:
                self.assertEqual(report["status"], "Failed")
                self.assertEqual(report["error"]["errorType"], "RuntimeError")
                self.assertEqual(calls, [])
            else:
                self.assertEqual(report["status"], "Completed")
                self.assertEqual(calls, [
                    "OPTIMIZE lhspark.Events", "OPTIMIZE lhspark.Scores",
                    "OPTIMIZE lhspark.ProcessingAudit", "OPTIMIZE lhspark.Quarantine",
                ])
                self.assertFalse(any("RawEvents" in call for call in calls))
                self.assertEqual(len(report["results"]), 4)


if __name__ == "__main__":
    unittest.main()
