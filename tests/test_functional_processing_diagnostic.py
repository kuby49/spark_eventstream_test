import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class FunctionalDiagnosticTests(unittest.TestCase):
    def test_production_prefix_and_tail_compile_without_production_consumer(self):
        source = (ROOT / "fabric/notebooks/streaming_pipeline.py").read_text()
        prefix = source.split('if RUN_MODE == "private-canary":')[0]
        tail = (ROOT / "fabric/notebooks/functional_processing_diagnostic_tail.py").read_text()
        combined = prefix + tail
        compile(combined, "functional-processing", "exec")
        self.assertIn("def process_batch", combined)
        self.assertNotIn('if RUN_MODE == "private-canary":', combined)

    def test_progress_alone_is_not_fixture_evidence(self):
        tail = (ROOT / "fabric/notebooks/functional_processing_diagnostic_tail.py").read_text()
        function = next(node for node in ast.parse(tail).body
                        if isinstance(node, ast.FunctionDef) and node.name == "process_fixture")
        code = ast.get_source_segment(tail, function)
        self.assertIn('F.col("run_id") == FIXTURE_RUN_ID', code)
        self.assertIn('F.col("message_id") == FIXTURE_MESSAGE_ID', code)
        self.assertIn("fixtureTerminalAuditObserved", code)
        self.assertNotIn('active.lastProgress["numInputRows"] > 0', code)

    def test_correlation_checks_values_and_terminal_failures(self):
        tail = (ROOT / "fabric/notebooks/functional_processing_diagnostic_tail.py").read_text()
        self.assertIn('audit[0]["status"] != "SCORED"', tail)
        self.assertIn("or quarantine", tail)
        self.assertIn("EXPECTED_AVERAGE", tail)
        self.assertIn('"oapEnabled": False', tail)
        self.assertIn('"acceptancePassed": False', tail)

    def test_runner_retains_compute_and_reuses_existing_notebook(self):
        runner = (ROOT / "scripts/Invoke-EventHubsAmqpDiagnostic.ps1").read_text()
        runtime = (ROOT / "scripts/Invoke-EventHubsAmqpPrivateRuntime.ps1").read_text()
        self.assertIn("[switch] $KeepRunning", runner)
        self.assertIn("if($KeepRunning)", runner.split("finally {")[1])
        self.assertIn("esehspark-kafka-oap-off-diagnostic", runtime)
        self.assertIn("no new item authorized", runtime)
        self.assertIn("$production[0].id)/updateDefinition", runtime)
        self.assertIn("Active production job prevents safe source update", runtime)

    def test_live_fixture_is_consumer_first_and_not_duplicate(self):
        tail = (ROOT / "fabric/notebooks/functional_processing_diagnostic_tail.py").read_text()
        runtime = (ROOT / "scripts/Invoke-EventHubsAmqpPrivateRuntime.ps1").read_text()
        self.assertIn("LIVE_FIXTURE", tail)
        self.assertIn("functional-ready-", tail)
        self.assertIn("active.lastProgress", tail)
        self.assertIn("Consumer readiness identity mismatch", runtime)
        self.assertLess(runtime.index("$report.consumerReadyBeforeSend=$ready"),
                        runtime.index("$stage='new_identifiable_high_fixture'"))
        self.assertIn("--high-only --message-id $FixtureMessageId --equipment-id 91", runtime)
        self.assertIn("All required processing must be Running", runtime)
        self.assertIn('"latest" if LIVE_FIXTURE else "earliest"', tail)
        self.assertIn('gate_stage("warm_output_writes", warm_output_writes)', tail)

    def test_unfiltered_progress_and_offsets_are_explicit_evidence(self):
        tail = (ROOT / "fabric/notebooks/functional_processing_diagnostic_tail.py").read_text()
        self.assertIn("beginningOffsets", tail)
        self.assertIn("endOffsets", tail)
        self.assertIn('"fixtureRunMatches"', tail)
        self.assertIn('"fixtureMessageMatches"', tail)
        self.assertIn("json.loads(progress.json)", tail)
        self.assertIn('"consumerObservations"', tail)

    def test_kql_failure_preserves_safe_classification(self):
        tail = (ROOT / "fabric/notebooks/functional_processing_diagnostic_tail.py").read_text()
        self.assertIn('timing["errorCode"] = str(error)', tail)
        source = (ROOT / "fabric/notebooks/streaming_pipeline.py").read_text()
        self.assertIn('reason = str(error) if isinstance(error, KqlQueryError)', source)


if __name__ == "__main__":
    unittest.main()
