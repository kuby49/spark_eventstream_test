import ast
from pathlib import Path
import types
import unittest


class DeltaWriteTests(unittest.TestCase):
    def test_static_pruning_retains_exact_pair_match_and_uses_typed_literals(self):
        source = (Path(__file__).resolve().parents[1]
                  / "fabric/notebooks/streaming_pipeline.py").read_text()
        node = next(node for node in ast.parse(source).body
                    if isinstance(node, ast.FunctionDef) and node.name == "_upsert")
        class Expression:
            def __init__(self, evaluate):
                self.evaluate = evaluate

            def __eq__(self, other):
                return Expression(lambda row: self.evaluate(row) == other.evaluate(row))

            def __and__(self, other):
                return Expression(lambda row: self.evaluate(row) and other.evaluate(row))

            def isin(self, values):
                return Expression(lambda row: self.evaluate(row) in values)

        class Merge:
            condition = None
            executed = False

            def alias(self, name):
                return self

            def merge(self, frame, condition):
                self.condition = condition
                return self

            def whenMatchedUpdateAll(self):
                return self

            def whenMatchedUpdate(self, set):
                self.updates = set
                return self

            def whenNotMatchedInsertAll(self):
                return self

            def execute(self):
                self.executed = True

        merge = Merge()
        frame = types.SimpleNamespace(alias=lambda name: "source", coalesce=lambda count: frame,
                                      columns=["run_id", "message_id", "spark_processed_at"])
        namespace = {
            "TABLE_EVENTS": "Events", "TABLE_SCORES": "Scores",
            "F": types.SimpleNamespace(
                col=lambda name: Expression(lambda row: row[name]),
                coalesce=lambda first, second: Expression(
                    lambda row: first.evaluate(row) if first.evaluate(row) is not None
                    else second.evaluate(row))),
            "spark": types.SimpleNamespace(
                table=lambda name: types.SimpleNamespace(schema="schema"),
                createDataFrame=lambda rows, schema: frame),
            "DeltaTable": types.SimpleNamespace(forName=lambda spark, name: merge),
        }
        exec(compile(ast.Module(body=[node], type_ignores=[]), "<delta-write>", "exec"), namespace)
        run = "untrusted'\\value"
        namespace["_upsert"]("table", [{"run_id": run, "message_id": "a"},
                                     {"run_id": "other", "message_id": "b"}])
        self.assertTrue(merge.executed)
        matches = lambda tr, tm, sr, sm: merge.condition.evaluate({
            "t.run_id": tr, "t.message_id": tm, "s.run_id": sr, "s.message_id": sm})
        self.assertTrue(matches(run, "a", run, "a"))
        self.assertFalse(matches(run, "a", "other", "b"))
        self.assertFalse(matches("outside", "a", "outside", "a"))
        self.assertFalse(matches(run, "outside", run, "outside"))
        for table in ("Events", "Scores"):
            namespace["_upsert"](table, [{"run_id": run, "message_id": "a"}])
            timestamp = merge.updates["spark_processed_at"]
            self.assertEqual(timestamp.evaluate({
                "t.spark_processed_at": "first", "s.spark_processed_at": "replay"}), "first")
            self.assertEqual(timestamp.evaluate({
                "t.spark_processed_at": None, "s.spark_processed_at": "new"}), "new")
