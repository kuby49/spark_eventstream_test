import contextlib
import io
import json
from pathlib import Path
import runpy
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch
from urllib.parse import urlsplit


SOURCE = Path(__file__).resolve().parents[1] / "fabric" / "notebooks" / "prerequisite_diagnostic.py"


class PrerequisiteDiagnosticTests(unittest.TestCase):
    def test_checkpoint_uses_hostname_not_https_uri(self):
        production = SOURCE.with_name("streaming_pipeline.py")
        checkpoint = next(
            statement for statement in ast.parse(production.read_text()).body
            if isinstance(statement, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "CHECKPOINT"
                    for target in statement.targets)
        )
        scope = {
            "WORKSPACE_ID": "workspace",
            "LAKEHOUSE_ID": "lakehouse",
            "RUN_ID": "run",
            "urlsplit": urlsplit,
            "notebookutils": SimpleNamespace(conf=SimpleNamespace(
                get=lambda key: "https://onelake.dfs.fabric.microsoft.com")),
        }
        exec(compile(ast.Module(body=[checkpoint], type_ignores=[]), str(production), "exec"), scope)
        self.assertEqual(scope["CHECKPOINT"],
                         "abfss://workspace@onelake.dfs.fabric.microsoft.com/"
                         "lakehouse/Files/checkpoints/run")

    def execute(self, *, fs_error=False, secret_error=False, mismatch=False):
        stored = {}
        fs = MagicMock()
        if fs_error:
            fs.mkdirs.side_effect = RuntimeError("unavailable")
            fs.head.side_effect = RuntimeError("unavailable")
        else:
            fs.put.side_effect = lambda path, value, overwrite: stored.update({path: value})
            fs.head.side_effect = lambda path: "[]" if mismatch else stored[path]
        credentials = MagicMock()
        credentials.getSecret.side_effect = lambda vault, name: (
            "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKey=NEVER_PRINT_THIS;EntityPath=test"
            if name == "eventstream-destination-connection" else "NEVER_PRINT_THIS")
        if secret_error:
            credentials.getSecret.side_effect = RuntimeError("NEVER_PRINT_THIS")
        notebook = MagicMock()
        utilities = SimpleNamespace(fs=fs, credentials=credentials, notebook=notebook,
                                    conf=SimpleNamespace(get=lambda key: "onelake.dfs.fabric.microsoft.com"))
        spark = MagicMock()
        spark.version = "test-runtime"
        spark.range.return_value.count.return_value = 1
        spark.table.return_value.where.return_value.count.return_value = 1
        output = io.StringIO()
        response = MagicMock()
        response.__enter__.return_value = response
        response.status = 201
        response.read.return_value = '{"score":83}'
        with contextlib.redirect_stdout(output), patch("urllib.request.urlopen", return_value=response), patch("socket.getaddrinfo", return_value=[
            (None, None, None, None, ("10.0.0.1", 443))
        ]):
            runpy.run_path(str(SOURCE), init_globals={"spark": spark, "notebookutils": utilities})
        notebook.exit.assert_called_once()
        result = json.loads(notebook.exit.call_args.args[0])
        self.assertNotIn("NEVER_PRINT_THIS", output.getvalue() + json.dumps(result))
        return result

    def test_successful_stages(self):
        result = self.execute()
        self.assertTrue(result["diagnosticOnly"])
        self.assertTrue(result["allDiagnosticStagesSucceeded"])
        self.assertFalse(result["privateKafkaRouteProven"])
        self.assertFalse(result["acceptancePassed"])
        self.assertEqual(len(result["results"]), 11)

    def test_storage_and_secret_errors_remain_retrievable_in_exit_value(self):
        result = self.execute(fs_error=True, secret_error=True)
        self.assertFalse(result["allDiagnosticStagesSucceeded"])
        failures = {item["stage"] for item in result["results"] if item["status"] == "Failed"}
        self.assertEqual(failures, {"onelake_readback", "checkpoint_abfss_readback",
                                    "destination_secret", "function_secret",
                                    "kafka_dns_only", "private_function_scoring"})

    def test_readback_mismatch_is_failure(self):
        result = self.execute(mismatch=True)
        self.assertFalse(result["allDiagnosticStagesSucceeded"])
        self.assertEqual(result["results"][1]["status"], "Failed")


if __name__ == "__main__":
    unittest.main()
import ast
