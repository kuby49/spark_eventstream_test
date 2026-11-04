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
