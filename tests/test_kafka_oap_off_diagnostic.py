import json
import pathlib
import unittest
from unittest.mock import Mock, patch


SOURCE = (pathlib.Path(__file__).parents[1] / "fabric" / "notebooks"
          / "kafka_oap_off_diagnostic.py").read_text()


class KafkaOapOffTests(unittest.TestCase):
    def run_notebook(self, dns_error=False, wrong_identity=False, stream_error=False):
        spark = Mock()
        spark.version = "test-runtime"
        spark.range.return_value.count.return_value = 1
        utilities = Mock()
        utilities.credentials.getSecret.return_value = (
            "Endpoint=sb://esehusw3qzfnf3vqakins22j.servicebus.windows.net/;"
            "EntityPath=" + ("wrong" if wrong_identity else "des_c4fa9b23-3070-4663-9473-dee28aace61c")
            + ";SharedAccessKeyName=test;SharedAccessKey=DO_NOT_LOG")
        query = spark.readStream.format.return_value.options.return_value.load.return_value.selectExpr.return_value.writeStream.format.return_value.queryName.return_value.option.return_value.trigger.return_value.start.return_value
        query.exception.return_value = None
        query.recentProgress = [{"numInputRows": 0}]
        if stream_error:
            query.awaitTermination.side_effect = RuntimeError("SaslAuthenticationException DO_NOT_LOG")
        with patch("socket.getaddrinfo", side_effect=OSError("DNS failed") if dns_error else None,
                   return_value=[(None, None, None, None, ("20.150.246.47", 9093))]):
            exec(compile(SOURCE, "<diagnostic>", "exec"),
                 {"spark": spark, "notebookutils": utilities})
        report = json.loads(utilities.notebook.exit.call_args.args[0])
        self.assertNotIn("DO_NOT_LOG", json.dumps(report))
        return report, spark, utilities, query

    def test_authorized_public_route_authentication_progress(self):
        report, spark, _, query = self.run_notebook()
        self.assertTrue(report["allDiagnosticStagesSucceeded"])
        self.assertTrue(report["kafkaConnectionAttempted"])
        self.assertFalse(report["acceptancePassed"])
        spark.readStream.format.assert_called_once_with("kafka")
        query.stop.assert_called_once()

    def test_dns_failure_prevents_secret_and_connection(self):
        report, spark, utilities, _ = self.run_notebook(dns_error=True)
        self.assertFalse(report["kafkaConnectionAttempted"])
        utilities.credentials.getSecret.assert_not_called()
        spark.readStream.format.assert_not_called()

    def test_wrong_endpoint_refused(self):
        report, spark, _, _ = self.run_notebook(wrong_identity=True)
        self.assertFalse(report["kafkaConnectionAttempted"])
        spark.readStream.format.assert_not_called()

    def test_stream_error_is_explicit_and_stopped(self):
        report, _, _, query = self.run_notebook(stream_error=True)
        self.assertFalse(report["allDiagnosticStagesSucceeded"])
        self.assertIn("SaslAuthenticationException", report["results"][-1]["errorCodes"])
        query.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
