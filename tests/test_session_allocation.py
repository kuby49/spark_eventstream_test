import ast
from datetime import datetime, timezone
import json
from pathlib import Path
import types
import unittest


ROOT = Path(__file__).resolve().parents[1]


class SessionAllocationTests(unittest.TestCase):
    def verify(self, changes=None, executors=1, require_executor=False):
        source = (ROOT / "fabric/notebooks/streaming_pipeline.py").read_text()
        node = next(node for node in ast.parse(source).body
                    if isinstance(node, ast.FunctionDef) and node.name == "_verify_session_allocation")
        configuration = {
            "spark.dynamicAllocation.enabled": "false", "spark.driver.cores": "8",
            "spark.driver.memory": "56g", "spark.executor.cores": "8",
            "spark.executor.memory": "56g", "spark.executor.instances": "1",
        }
        configuration.update(changes or {})
        jsc = types.SimpleNamespace(getExecutorMemoryStatus=lambda: types.SimpleNamespace(
            size=lambda: executors + 1))
        spark = types.SimpleNamespace(sparkContext=types.SimpleNamespace(
            getConf=lambda: configuration, _jsc=types.SimpleNamespace(sc=lambda: jsc)))
        namespace = {"spark": spark, "datetime": datetime, "timezone": timezone}
        exec(compile(ast.Module(body=[node], type_ignores=[]), "<allocation>", "exec"), namespace)
        return namespace["_verify_session_allocation"](require_executor=require_executor)

    def test_fixed_session_proves_observed_cores_without_expanding_resource_envelope(self):
        result = self.verify(require_executor=True)
        self.assertEqual(result["maximumDriverAndExecutorCores"], 16)
        self.assertEqual(result["activeExecutors"], 1)

    def test_ignored_dynamic_setting_or_larger_resource_count_fails_closed(self):
        for change in ({"spark.dynamicAllocation.enabled": "true"},
                       {"spark.executor.instances": "2"},
                       {"spark.executor.cores": "16"},
                       {"spark.driver.memory": "112g"}):
            with self.subTest(change=change), self.assertRaisesRegex(
                    RuntimeError, "FixedExecutorConfigurationNotApplied"):
                self.verify(change)
        with self.assertRaisesRegex(RuntimeError, "FixedExecutorCountMismatch"):
            self.verify(executors=2)

    def test_executor_can_register_during_warmup_but_is_required_before_sends(self):
        self.assertEqual(self.verify(executors=0)["activeExecutors"], 0)
        with self.assertRaisesRegex(RuntimeError, "FixedExecutorCountMismatch"):
            self.verify(executors=0, require_executor=True)

    def test_shared_first_cell_is_preserved_in_diagnostic_and_verified_before_readiness(self):
        configuration = json.loads(
            (ROOT / "fabric/notebooks/spark_session_configuration.json").read_text())
        self.assertEqual(configuration["numExecutors"], 1)
        self.assertEqual(configuration["conf"], {"spark.dynamicAllocation.enabled": "false"})
        runner = (ROOT / "scripts/Invoke-EventHubsAmqpDiagnostic.ps1").read_text()
        self.assertIn("$package.cells = $configurationCells +", runner)
        baseline = (ROOT / "fabric/notebooks/baseline_diagnostic_tail.py").read_text()
        self.assertLess(baseline.index("_verify_session_allocation(require_executor=True)"),
                        baseline.index('f"Files/diagnostics/functional-ready-'))


if __name__ == "__main__":
    unittest.main()
