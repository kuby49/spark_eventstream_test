from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class FaultDiagnosticTests(unittest.TestCase):
    def test_write_profile_compiles_and_only_rewrites_unchanged_retained_rows(self):
        source = (ROOT / "fabric/notebooks/streaming_pipeline.py").read_text()
        helpers = (ROOT / "fabric/notebooks/functional_processing_diagnostic_tail.py").read_text()
        profile = (ROOT / "fabric/notebooks/write_profile_diagnostic_tail.py").read_text()
        compile(source.split('if RUN_MODE == "private-canary":')[0]
                + helpers.split("gate_output_records = {}")[0] + profile, "<write-profile>", "exec")
        for field in ('"noKafkaSends": True', '"noApiCalls": True', '"latencyAcceptance": False'):
            self.assertIn(field, profile)

    def test_compaction_profile_preserves_all_rows_and_requires_explicit_mode(self):
        profile = (ROOT / "fabric/notebooks/write_profile_diagnostic_tail.py").read_text()
        runner = (ROOT / "scripts/Invoke-EventHubsAmqpDiagnostic.ps1").read_text()
        self.assertIn('if($OptimizeWriteProfile -and -not $WriteProfile)', runner)
        self.assertIn('spark.sql(f"OPTIMIZE {table}")', profile)
        self.assertIn("before_count != after_count", profile)
        self.assertIn("before.exceptAll(after)", profile)
        self.assertIn("after.exceptAll(before)", profile)
        self.assertIn('"allRowsUnchanged": True', profile)
        self.assertNotIn("VACUUM", profile)

    def test_sustained_diagnostic_is_not_full_baseline_and_has_correct_window_aliases(self):
        runner = (ROOT / "scripts/Invoke-EventHubsAmqpDiagnostic.ps1").read_text()
        tail = (ROOT / "fabric/notebooks/baseline_diagnostic_tail.py").read_text()
        self.assertIn("if($SustainedDiagnostic){5}", runner)
        self.assertIn('"baselineRun": not LOAD_TEST and not SUSTAINED_DIAGNOSTIC', tail)
        self.assertNotIn("summarize first=", tail)
        self.assertIn("project WindowStart", tail)
        self.assertIn("project WindowEnd", tail)

    def test_fault_tail_compiles_with_production_helpers_and_declares_scope(self):
        source = (ROOT / "fabric/notebooks/streaming_pipeline.py").read_text()
        helpers = (ROOT / "fabric/notebooks/functional_processing_diagnostic_tail.py").read_text()
        fault = (ROOT / "fabric/notebooks/fault_processing_diagnostic_tail.py").read_text()
        compile(source.split('if RUN_MODE == "private-canary":')[0]
                + helpers.split("gate_output_records = {}")[0] + fault, "<fault-diagnostic>", "exec")
        for field in ('"noKafkaSends": True', '"noLatencyClaim": True', '"acceptancePassed": False'):
            self.assertIn(field, fault)
        self.assertIn("statuses == [201, 200]", fault)
        self.assertIn("urllib.request.urlopen = fault_original_urlopen", fault)
        self.assertIn("globals()[\"_upsert\"] = fault_original_upsert", fault)
        self.assertIn('fault_original_query("print fault = esehspark_missing_fault_identifier", {})', fault)
        self.assertIn('"actualServiceError": "KQLHttp400"', fault)
        self.assertIn('"clientRequestId"', fault)
        self.assertIn('"serviceRequestId"', fault)

    def test_fault_matrix_requires_explicit_keep_running_and_forbids_sends(self):
        runner = (ROOT / "scripts/Invoke-EventHubsAmqpDiagnostic.ps1").read_text()
        self.assertIn("FaultMatrix requires functional-processing and KeepRunning", runner)
        self.assertIn("$Baseline -or $LiveHighFixture -or $ReuseLastHighFixture", runner)

    def test_explicit_replay_tail_uses_original_key_and_no_latency_claim(self):
        source = (ROOT / "fabric/notebooks/streaming_pipeline.py").read_text()
        helpers = (ROOT / "fabric/notebooks/functional_processing_diagnostic_tail.py").read_text()
        replay = (ROOT / "fabric/notebooks/quarantine_replay_diagnostic_tail.py").read_text()
        compile(source.split('if RUN_MODE == "private-canary":')[0]
                + helpers.split("gate_output_records = {}")[0] + replay, "<quarantine-replay>", "exec")
        self.assertIn("replay_quarantined(REPLAY_RUN_ID, REPLAY_MESSAGE_ID)", replay)
        self.assertIn('"noKafkaSends": True', replay)
        self.assertIn('"noLatencyClaim": True', replay)
