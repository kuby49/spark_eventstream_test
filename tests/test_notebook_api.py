import ast
import io
import json
from pathlib import Path
import random
import time
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch


def load_helpers():
    tree = ast.parse((Path(__file__).parents[1] / "fabric/notebooks/streaming_pipeline.py").read_text())
    nodes = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef))
             and node.name in ("ApiRequestError", "_call_score_api")]
    namespace = {"json": json, "time": time, "random": random, "urllib": __import__("urllib"),
                 "API_URL": "https://safe.example/api/score", "FUNCTION_KEY": "FAKE"}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<notebook-api>", "exec"), namespace)
    return namespace


class NotebookApiTests(unittest.TestCase):
    event = {"message_id": "message", "run_id": "run", "equipment_id": 17,
             "timestamp": "2026-11-03T00:00:00Z"}

    @staticmethod
    def response(score=37):
        return io.BytesIO(json.dumps({"message_id": "message", "run_id": "run",
                                     "score": score}).encode())

    def test_transient_503_retries_same_identity_and_payload(self):
        ns = load_helpers()
        transient = urllib.error.HTTPError("https://safe.example", 503, "failed", {}, io.BytesIO())
        self.addCleanup(transient.close)
        with patch("urllib.request.urlopen", side_effect=[
                transient,
                self.response()]) as request, patch("time.sleep"):
            self.assertEqual(ns["_call_score_api"](self.event, 95), (37, 2))
        first, second = [call.args[0] for call in request.call_args_list]
        self.assertEqual(first.data, second.data)
        self.assertEqual(first.get_header("Idempotency-key"), second.get_header("Idempotency-key"))

    def test_invalid_integer_score_never_retries(self):
        ns = load_helpers()
        for score in (True, 0, 101, 2.5):
            with self.subTest(score=score), patch("urllib.request.urlopen", return_value=self.response(score)) as request:
                with self.assertRaisesRegex(ns["ApiRequestError"], "InvalidScoringResponse"):
                    ns["_call_score_api"](self.event, 95)
                self.assertEqual(request.call_count, 1)

    def test_payload_conflict_is_terminal(self):
        ns = load_helpers()
        conflict = urllib.error.HTTPError("https://safe.example", 409, "conflict", {}, io.BytesIO())
        self.addCleanup(conflict.close)
        with patch("urllib.request.urlopen", side_effect=conflict) as request:
            with self.assertRaisesRegex(ns["ApiRequestError"], "HTTP409"):
                ns["_call_score_api"](self.event, 95)
        self.assertEqual(request.call_count, 1)

    def test_response_completed_after_eight_second_budget_is_not_success(self):
        ns = load_helpers()
        with patch("urllib.request.urlopen", return_value=self.response()), \
                patch("time.monotonic", side_effect=[0, 0, 9]):
            with self.assertRaisesRegex(ns["ApiRequestError"], "DeadlineExceeded"):
                ns["_call_score_api"](self.event, 95)

    def test_transport_timeout_is_at_most_two_attempts(self):
        ns = load_helpers()
        with patch("urllib.request.urlopen", side_effect=TimeoutError()) as request, patch("time.sleep"):
            with self.assertRaisesRegex(ns["ApiRequestError"], "TransportError") as error:
                ns["_call_score_api"](self.event, 95)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(error.exception.attempts, 2)
