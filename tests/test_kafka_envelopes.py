import ast
import json
from pathlib import Path
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "fabric/notebooks/streaming_pipeline.py"


class KafkaEnvelopeTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse(SOURCE.read_text())
        function = next(node for node in tree.body
                        if isinstance(node, ast.FunctionDef) and node.name == "decode_kafka_envelope")
        namespace = {"json": json}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(SOURCE), "exec"), namespace)
        self.decode = namespace["decode_kafka_envelope"]

    def row(self, body):
        return {"value": body, "topic": "derived", "partition": 0, "offset": 9, "timestamp": "now"}

    def test_array_yields_each_logical_event_with_broker_metadata(self):
        records = [{"message_id": "one", "run_id": "run", "temperature": 95},
                   {"message_id": "two", "run_id": "run", "temperature": 96}]
        rows = self.decode(self.row(json.dumps(records)))
        self.assertEqual([row["parsed"] for row in rows], records)
        self.assertEqual([json.loads(row["value"]) for row in rows], records)
        self.assertTrue(all(row["partition"] == 0 and row["offset"] == 9 for row in rows))
        self.assertEqual([row["envelope_index"] for row in rows], [0, 1])

    def test_single_object_remains_supported(self):
        self.assertEqual(self.decode(self.row('{"temperature":95}'))[0]["parsed"], {"temperature": 95})

    def test_malformed_and_nonobject_inputs_reach_validation(self):
        for body in ("not json", "[]", "null", "42", '[{"message_id":"valid"},42]'):
            rows = self.decode(self.row(body))
            self.assertTrue(rows)
            self.assertTrue(all("parsed" in row for row in rows))
        self.assertIsNone(self.decode(self.row("not json"))[0]["parsed"])

    def test_canonical_event_hash_input_is_stable_across_envelopes(self):
        direct = self.decode(self.row('{"b":2,"a":1}'))[0]
        array = self.decode(self.row('[{"a":1,"b":2}]'))[0]
        self.assertEqual(direct["value"], array["value"])

    def test_mixed_runs_and_invalid_element_survive_expansion(self):
        body = '[{"run_id":"one"},{"run_id":"two"},42]'
        rows = self.decode(self.row(body))
        self.assertEqual([row["parsed"] for row in rows], [{"run_id": "one"}, {"run_id": "two"}, 42])
        tree = ast.parse(SOURCE.read_text())
        process = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "process_batch")
        code = ast.get_source_segment(SOURCE.read_text(), process)
        self.assertLess(code.index("decode_kafka_envelope"), code.index("fixture_run_id is not None"))
        self.assertIn("isinstance(event, dict) and event.get", code)


if __name__ == "__main__":
    unittest.main()
