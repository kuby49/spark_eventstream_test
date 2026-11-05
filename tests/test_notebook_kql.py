import ast
from datetime import datetime, timezone
import io
import json
import math
import pathlib
import random
import time
import unittest
import urllib.request
import uuid
from unittest.mock import Mock, patch


SOURCE = (pathlib.Path(__file__).parents[1] / "fabric" / "notebooks"
          / "streaming_pipeline.py").read_text()


def load_helpers():
    tree = ast.parse(SOURCE)
    names = {"KqlQueryError", "_query_kusto", "_rows_from_kusto", "_eventhouse_average", "_utcnow"}
    module = ast.Module(body=[
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names
    ], type_ignores=[])
    namespace = {"json": json, "datetime": datetime, "time": time, "random": random,
                 "urllib": __import__("urllib"), "uuid": uuid, "math": math, "timezone": timezone,
                 "KUSTO_ENDPOINT": "https://safe.example", "KUSTO_DATABASE": "ehspark",
                 "_kusto_token": lambda: "FAKE"}
    exec(compile(module, "<kql-helpers>", "exec"), namespace)
    return namespace


class NotebookKqlTests(unittest.TestCase):
    def test_http_error_capture_is_bounded_and_sanitized_without_changing_failure(self):
        helpers = load_helpers()
        body = json.dumps({"error": {"code": "TooManyRequests",
                          "@type": "Kusto.QueryThrottledException",
                          "message": "SECRET principal user@example.test Capacity: 50 "
                                     "Origin: RequestRateLimitPolicy/WorkloadGroup/SECRET",
                          "token": "SECRET"}}).encode()
        diagnostics = {}
        failure = urllib.error.HTTPError("https://safe.example", 429, "failed",
                                        {"Retry-After": "0"}, io.BytesIO(body))
        with patch("urllib.request.urlopen", side_effect=failure), patch("time.sleep"):
            with self.assertRaisesRegex(helpers["KqlQueryError"], "KQLHttp429") as caught:
                helpers["_query_kusto"]("query", {}, diagnostics=diagnostics)
        self.assertEqual(caught.exception.attempts, 2)
        captured = diagnostics["requests"][0]["errorBody"]
        self.assertEqual(captured["code"], "TooManyRequests")
        self.assertEqual(captured["capacityLimit"], 50)
        self.assertEqual(captured["policyCategories"], ["RequestRateLimitPolicy"])
        self.assertEqual(diagnostics["requests"][0]["headers"]["Retry-After"], "0")
        self.assertNotIn("SECRET", str(diagnostics))
        self.assertNotIn("user@example", str(diagnostics))
        failure.close()

    def test_http_error_capture_oversized_invalid_and_read_failure_are_explicit(self):
        for body, expected in ((b"x" * 9000, "Oversized"), (b"not json SECRET", "CaptureUnavailable")):
            helpers = load_helpers()
            diagnostics = {}
            failure = urllib.error.HTTPError("https://safe.example", 403, "failed", {}, io.BytesIO(body))
            with patch("urllib.request.urlopen", side_effect=failure):
                with self.assertRaisesRegex(helpers["KqlQueryError"], "KQLHttp403"):
                    helpers["_query_kusto"]("query", {}, diagnostics=diagnostics)
            captured = diagnostics["requests"][0]["errorBody"]
            self.assertEqual(captured["status"], expected)
            self.assertLessEqual(captured.get("bytesRead", 0), 8193)
            self.assertNotIn("SECRET", str(diagnostics))
            failure.close()

    def test_http_error_body_read_failure_does_not_mask_http_status(self):
        helpers = load_helpers()
        diagnostics = {}
        failure = urllib.error.HTTPError("https://safe.example", 403, "failed", {}, io.BytesIO())
        with patch.object(failure, "read1", side_effect=TimeoutError()), \
                patch("urllib.request.urlopen", side_effect=failure):
            with self.assertRaisesRegex(helpers["KqlQueryError"], "KQLHttp403"):
                helpers["_query_kusto"]("query", {}, diagnostics=diagnostics)
        self.assertEqual(diagnostics["requests"][0]["errorBody"]["errorType"], "TimeoutError")
        self.assertEqual(diagnostics["requests"][0]["errorBody"]["status"], "CaptureUnavailable")
        failure.close()
    def test_readonly_warmup_retries_only_transient_errors_at_most_three_times(self):
        helpers = load_helpers()
        tail = (pathlib.Path(__file__).parents[1] / "fabric/notebooks"
                / "functional_processing_diagnostic_tail.py").read_text()
        node = next(node for node in ast.parse(tail).body
                    if isinstance(node, ast.FunctionDef) and node.name == "warm_kql_transport")
        exec(compile(ast.Module(body=[node], type_ignores=[]), "<warm-kql>", "exec"), helpers)
        ready = {"Tables": [{"Columns": [{"ColumnName": "ready"}], "Rows": [[1]]}]}
        query = Mock(side_effect=[helpers["KqlQueryError"]("KQLTransportError"), ready])
        helpers["_query_kusto"] = query
        with patch("time.sleep"):
            self.assertEqual(helpers["warm_kql_transport"]()["warmupAttempts"], 2)
        self.assertEqual(query.call_count, 2)
        for code, count in (("KQLTransportError", 3), ("KQLHttp400", 1)):
            query = Mock(side_effect=helpers["KqlQueryError"](code))
            helpers["_query_kusto"] = query
            with patch("time.sleep"), self.assertRaises(helpers["KqlQueryError"]):
                helpers["warm_kql_transport"]()
            self.assertEqual(query.call_count, count)

    def test_documented_rest_properties_are_json_string_with_scalar_values(self):
        helpers = load_helpers()
        response = io.BytesIO(b'{"Tables":[]}')
        with patch("urllib.request.urlopen", return_value=response) as request:
            helpers["_query_kusto"]("declare query_parameters(n:int); print n",
                                   {"n": 17, "flag": True, "at": datetime(2026, 11, 3, tzinfo=timezone.utc)})
        body = json.loads(request.call_args.args[0].data)
        self.assertIsInstance(body["properties"], str)
        properties = json.loads(body["properties"])
        self.assertEqual(properties["Parameters"]["n"], "17")
        self.assertEqual(properties["Parameters"]["flag"], "true")
        self.assertEqual(properties["Parameters"]["at"], "datetime(2026-11-03T00:00:00+00:00)")
        self.assertTrue(all(isinstance(value, str) for value in properties["Parameters"].values()))

    def test_selects_projection_not_first_v1_table(self):
        helpers = load_helpers()
        result = {"Tables": [
            {"Columns": [{"ColumnName": "info"}], "Rows": [["ignore"]]},
            {"TableName": "Table_1", "Columns": [{"ColumnName": "visible"}], "Rows": [[1]]},
        ]}
        self.assertEqual(helpers["_rows_from_kusto"](result, ["visible"]), [[1]])
        with self.assertRaises(helpers["KqlQueryError"]):
            helpers["_rows_from_kusto"](result, ["average", "unique_count"])

    def test_error_or_invalid_projection_never_empty_success(self):
        helpers = load_helpers()
        for result in ({"error": "failed"}, {"Tables": []}, {"Tables": [{
            "Columns": [{"ColumnName": "visible"}], "Rows": [[1, 2]]}]}):
            with self.assertRaises(helpers["KqlQueryError"]):
                helpers["_rows_from_kusto"](result, ["visible"])

    def test_transport_timeout_retries_within_four_second_budget(self):
        helpers = load_helpers()
        with patch("urllib.request.urlopen", side_effect=[
                TimeoutError(), io.BytesIO(b'{"Tables":[]}')]) as request, \
                patch("time.sleep"):
            helpers["_query_kusto"]("query", {}, timeout_seconds=4.0)
        self.assertEqual(request.call_count, 2)
        self.assertTrue(all(call.kwargs["timeout"] <= 2.0 for call in request.call_args_list))

    def test_completed_response_after_total_budget_is_not_success(self):
        helpers = load_helpers()
        with patch("urllib.request.urlopen", return_value=io.BytesIO(b'{"Tables":[]}')), \
                patch("time.monotonic", side_effect=[0, 0, 5]):
            with self.assertRaisesRegex(helpers["KqlQueryError"], "KQLDeadlineExceeded") as caught:
                helpers["_query_kusto"]("query", {}, timeout_seconds=4.0)
        self.assertEqual(caught.exception.cause, "ResponseCompletedAfterDeadline")

    def test_token_time_is_included_in_budget_and_no_http_starts_after_expiry(self):
        helpers = load_helpers()
        with patch("urllib.request.urlopen") as request, \
                patch("time.monotonic", side_effect=[0, 5]):
            with self.assertRaisesRegex(helpers["KqlQueryError"], "KQLDeadlineExceeded"):
                helpers["_query_kusto"]("query", {}, timeout_seconds=4.0)
        request.assert_not_called()

    def test_phase_diagnostics_preserve_late_parsed_response_failure(self):
        helpers = load_helpers()
        clock = [0.0]
        response = io.BytesIO(b'{"Tables":[]}')
        response.status = 200
        response.headers = {"x-ms-request-id": "safe-request",
                            "Authorization": "SECRET", "Set-Cookie": "SECRET"}
        original_loads = json.loads

        def delayed_parse(value):
            clock[0] += 5
            return original_loads(value)

        diagnostics = {}
        with patch("urllib.request.urlopen", return_value=response), \
                patch("time.monotonic", side_effect=lambda: clock[0]), \
                patch("json.loads", side_effect=delayed_parse):
            with self.assertRaisesRegex(helpers["KqlQueryError"], "KQLDeadlineExceeded") as caught:
                helpers["_query_kusto"]("query", {}, diagnostics=diagnostics)
        self.assertEqual(caught.exception.cause, "ResponseCompletedAfterDeadline")
        self.assertEqual(caught.exception.attempts, 1)
        self.assertTrue(diagnostics["responseCompletedAfterDeadline"])
        self.assertEqual([phase["phase"] for phase in diagnostics["phases"]],
                         ["token_acquisition", "request_setup", "urlopen_response",
                          "response_read", "response_decode", "response_parse",
                          "postresponse_deadline_check"])
        self.assertEqual(diagnostics["phases"][-2]["elapsedSeconds"], 5)
        self.assertEqual(diagnostics["requests"][0]["httpStatus"], 200)
        self.assertEqual(diagnostics["requests"][0]["headers"], {"x-ms-request-id": "safe-request"})
        self.assertNotIn("SECRET", str(diagnostics))
        self.assertIsNone(diagnostics["serverDurationSeconds"])

    def test_phase_diagnostics_preserve_expired_credential_budget(self):
        helpers = load_helpers()
        clock = [0.0]

        def token():
            clock[0] += 5
            return "SECRET"

        helpers["_kusto_token"] = token
        diagnostics = {}
        with patch("time.monotonic", side_effect=lambda: clock[0]), \
                patch("urllib.request.urlopen") as request:
            with self.assertRaisesRegex(helpers["KqlQueryError"], "KQLDeadlineExceeded") as caught:
                helpers["_query_kusto"]("query", {}, diagnostics=diagnostics)
        request.assert_not_called()
        self.assertEqual(caught.exception.attempts, 0)
        self.assertEqual(diagnostics["phases"][0]["elapsedSeconds"], 5)
        self.assertNotIn("SECRET", str(diagnostics))

    def test_phase_diagnostics_preserve_success_and_transport_retry(self):
        helpers = load_helpers()
        diagnostics = {}
        with patch("urllib.request.urlopen", side_effect=[
                TimeoutError(), io.BytesIO(b'{"Tables":[]}')]) as request, patch("time.sleep"):
            result = helpers["_query_kusto"]("query", {}, diagnostics=diagnostics)
        self.assertEqual(result, {"Tables": []})
        self.assertEqual(request.call_count, 2)
        self.assertFalse(diagnostics["responseCompletedAfterDeadline"])
        self.assertEqual(len(diagnostics["requests"]), 2)
        self.assertNotEqual(diagnostics["requests"][0]["clientRequestId"],
                            diagnostics["requests"][1]["clientRequestId"])
        opens = [entry for entry in diagnostics["phases"] if entry["phase"] == "urlopen_response"]
        self.assertEqual([entry["completed"] for entry in opens], [False, True])

    def test_successful_empty_average_does_not_require_current_trigger_visibility(self):
        helpers = load_helpers()
        helpers["_query_kusto"] = Mock(return_value={"Tables": [{
            "Columns": [{"ColumnName": name} for name in
                        ("average", "unique_count", "trigger_visible", "window_records")],
            "Rows": [[None, 0, 0, "[]"]]}]})
        average, count, visible, snapshot = helpers["_eventhouse_average"]({
            "run_id": "run", "message_id": "message", "equipment_id": 17,
            "timestamp": "2026-11-03T00:00:00Z"})
        self.assertIsNone(average)
        self.assertEqual(count, 0)
        self.assertIsNone(visible)
        self.assertIsInstance(snapshot, datetime)
        helpers["_query_kusto"].assert_called_once()
        query, parameters = helpers["_query_kusto"].call_args.args
        self.assertIn("timestamp between (triggerTime - 20m .. triggerTime)", query)
        self.assertIn("by run_id, message_id", query)
        self.assertEqual(parameters["equipmentId"], 17)

    def test_available_average_is_used_even_when_current_trigger_has_not_ingested(self):
        helpers = load_helpers()
        helpers["_query_kusto"] = Mock(return_value={"Tables": [{
            "Columns": [{"ColumnName": name} for name in
                        ("average", "unique_count", "trigger_visible", "window_records")],
            "Rows": [[20.0, 1, 0, [{"run_id": "prior", "message_id": "prior",
                                   "temperature": 20.0, "timestamp": "2026-11-02T23:59:00Z"}]]]}]})
        average, count, visible, _ = helpers["_eventhouse_average"]({
            "run_id": "run", "message_id": "message", "equipment_id": 17,
            "timestamp": "2026-11-03T00:00:00Z"})
        self.assertEqual((average, count, visible), (20.0, 1, None))

    def test_real_kusto_empty_nan_sentinel_is_empty_only_with_zero_count_and_members(self):
        helpers = load_helpers()
        event = {"run_id": "run", "message_id": "message", "equipment_id": 17,
                 "timestamp": "2000-01-01T00:20:00Z"}
        for value in ("NaN", float("nan")):
            response = {"Tables": [{
                "Columns": [{"ColumnName": name} for name in
                            ("average", "unique_count", "trigger_visible", "window_records")],
                "Rows": [[value, 0, 0, []]]}]}
            helpers["_query_kusto"] = Mock(return_value=response)
            self.assertIsNone(helpers["_eventhouse_average"](event)[0])
            response["Tables"][0]["Rows"][0][1] = 1
            with self.assertRaises(helpers["KqlQueryError"]):
                helpers["_eventhouse_average"](event)

    def test_actual_query_error_is_not_a_successful_empty_average(self):
        helpers = load_helpers()
        helpers["_query_kusto"] = Mock(side_effect=helpers["KqlQueryError"]("KQLHttp403"))
        with self.assertRaisesRegex(helpers["KqlQueryError"], "KQLHttp403"):
            helpers["_eventhouse_average"]({
                "run_id": "run", "message_id": "message", "equipment_id": 17,
                "timestamp": "2026-11-03T00:00:00Z"})


if __name__ == "__main__":
    unittest.main()
