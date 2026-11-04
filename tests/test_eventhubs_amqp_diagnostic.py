import json
import pathlib
import unittest
from unittest.mock import Mock, patch


SOURCE = (pathlib.Path(__file__).parents[1] / "fabric" / "notebooks"
          / "eventhubs_amqp_diagnostic.py").read_text()


class EventHubsDiagnosticTests(unittest.TestCase):
    def run_notebook(self, addresses, connector_present=True):
        spark = Mock()
        spark.version = "test-runtime"
        spark.range.return_value.count.return_value = 1
        loader = spark._jvm.java.lang.Thread.currentThread.return_value.getContextClassLoader.return_value
        if not connector_present:
            loader.loadClass.side_effect = RuntimeError("ClassNotFoundException")
        utilities = Mock()
        entries = [(None, None, None, None, (address, 5671)) for address in addresses]
        with patch("socket.getaddrinfo", return_value=entries):
            exec(compile(SOURCE, "<diagnostic>", "exec"),
                 {"spark": spark, "notebookutils": utilities})
        report = json.loads(utilities.notebook.exit.call_args.args[0])
        return report, spark, utilities

    def test_public_dns_refuses_credentials_and_connection(self):
        report, spark, utilities = self.run_notebook(["20.150.246.47"])
        self.assertFalse(report["amqpConnectionAttempted"])
        self.assertFalse(report["allDiagnosticStagesSucceeded"])
        utilities.credentials.getSecret.assert_not_called()
        spark.readStream.format.assert_not_called()

    def test_mixed_dns_refused(self):
        report, _, _ = self.run_notebook(["10.74.2.15", "20.150.246.47"])
        self.assertFalse(report["amqpConnectionAttempted"])

    def test_missing_connector_explicit_and_no_secret(self):
        report, _, utilities = self.run_notebook(["10.250.0.9"], connector_present=False)
        stage = next(item for item in report["results"] if item["stage"] == "native_eventhubs_connector")
        self.assertEqual(stage["status"], "Failed")
        self.assertIn("ClassNotFoundException", stage["errorCodes"])
        utilities.credentials.getSecret.assert_not_called()

    def test_dns_failure_does_not_connect(self):
        spark = Mock()
        spark.version = "test-runtime"
        spark.range.return_value.count.return_value = 1
        utilities = Mock()
        with patch("socket.getaddrinfo", side_effect=OSError("DNS blocked")):
            exec(compile(SOURCE, "<diagnostic>", "exec"),
                 {"spark": spark, "notebookutils": utilities})
        report = json.loads(utilities.notebook.exit.call_args.args[0])
        self.assertFalse(report["amqpConnectionAttempted"])
        self.assertFalse(report["allDiagnosticStagesSucceeded"])
        self.assertEqual(report["results"][-1]["errorType"], "OSError")
        utilities.credentials.getSecret.assert_not_called()

    def test_runtime_keeps_private_management_and_finally_shutdown(self):
        root = pathlib.Path(__file__).parents[1]
        runtime = (root / "scripts" / "Invoke-EventHubsAmqpPrivateRuntime.ps1").read_text()
        runner = (root / "scripts" / "Invoke-EventHubsAmqpDiagnostic.ps1").read_text()
        self.assertIn("Refusing nonprivate Fabric API route.", runtime)
        self.assertIn("MaximumRedirection=0", runtime)
        self.assertNotIn("communicationPolicy", runtime)
        self.assertIn("finally {", runner)
        self.assertIn("'vm','deallocate'", runner)
        self.assertIn("$capacityId/suspend?", runner)
        self.assertIn("Export-PreservedDeploymentEvidence.ps1", runner)
        self.assertNotIn("Set-FabricNotebookPackage.ps1", runner)
        self.assertIn("esehspark-$DiagnosticKind-diagnostic", runtime)

    def test_private_guard_then_bounded_native_stream_and_stop(self):
        spark = Mock()
        spark.version = "test-runtime"
        spark.range.return_value.count.return_value = 1
        utilities = Mock()
        utilities.credentials.getSecret.return_value = (
            "Endpoint=sb://esehusw3qzfnf3vqakins22j.servicebus.windows.net/;"
            "EntityPath=des_c4fa9b23-3070-4663-9473-dee28aace61c;"
            "SharedAccessKeyName=test;SharedAccessKey=DO_NOT_LOG")
        query = spark.readStream.format.return_value.options.return_value.load.return_value.selectExpr.return_value.writeStream.format.return_value.queryName.return_value.option.return_value.trigger.return_value.start.return_value
        query.exception.return_value = None
        query.recentProgress = [{"numInputRows": 1}]
        with patch("socket.getaddrinfo", return_value=[(None, None, None, None, ("10.250.0.9", 5671))]):
            exec(compile(SOURCE, "<diagnostic>", "exec"), {"spark": spark, "notebookutils": utilities})
        report = json.loads(utilities.notebook.exit.call_args.args[0])
        self.assertTrue(report["amqpConnectionAttempted"])
        self.assertTrue(report["allDiagnosticStagesSucceeded"])
        query.awaitTermination.assert_called_once_with(45)
        query.stop.assert_called_once()
        self.assertNotIn("DO_NOT_LOG", json.dumps(report))
        spark.readStream.format.assert_called_once_with("eventhubs")
        self.assertFalse(report["acceptancePassed"])


if __name__ == "__main__":
    unittest.main()
