import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class IndependentReaderTests(unittest.TestCase):
    def setUp(self):
        arrow = types.ModuleType("pyarrow")
        parquet = types.ModuleType("pyarrow.parquet")
        arrow.parquet = parquet
        spec = importlib.util.spec_from_file_location(
            "observer_test", ROOT / "src/reconciliation/private_delta_observer.py")
        self.module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"pyarrow": arrow, "pyarrow.parquet": parquet}):
            spec.loader.exec_module(self.module)
        self.reader = object.__new__(self.module.PrivateDeltaReader)
        self.reader.logs, self.reader.files, self.reader.parquet = {}, {}, {}
        self.reader.list = lambda path: [{"name": "table/_delta_log/00000000000000000000.json"}]
        self.table = types.SimpleNamespace(to_pylist=lambda: [{"message_id": "id", "score": 95}])
        self.module.pq.read_table = lambda data: self.table

    def commit(self, actions):
        content = "\n".join(json.dumps(action) for action in actions).encode()
        self.reader.get = lambda path: content if path.endswith(".json") else b"parquet"

    def test_committed_files_only_and_remove_is_honored(self):
        self.commit([{"add": {"path": "removed.parquet"}},
                     {"remove": {"path": "removed.parquet"}},
                     {"add": {"path": "active.parquet"}}])
        self.assertEqual(self.reader.rows("table"), [{"message_id": "id", "score": 95}])
        self.assertEqual(set(self.reader.files["table"]), {"active.parquet"})

    def test_supported_fabric_protocol_without_active_deletion_vectors(self):
        self.commit([{"protocol": {"minReaderVersion": 3, "readerFeatures": ["deletionVectors"]}},
                     {"add": {"path": "active.parquet"}}])
        self.assertEqual(len(self.reader.rows("table")), 1)

    def test_active_deletion_vector_fails_closed(self):
        self.commit([{"add": {"path": "active.parquet", "deletionVector": {"storageType": "i"}}}])
        with self.assertRaisesRegex(RuntimeError, "UnsupportedDeltaDeletionVector"):
            self.reader.rows("table")

    def test_unknown_protocol_and_column_mapping_fail_closed(self):
        for action in ({"protocol": {"minReaderVersion": 4}},
                       {"protocol": {"minReaderVersion": 3, "readerFeatures": ["unknown"]}},
                       {"metaData": {"configuration": {"delta.columnMapping.mode": "name"}}}):
            self.commit([action])
            with self.assertRaises(RuntimeError):
                self.reader.rows("table")

    def test_missing_commit_history_not_silently_accepted(self):
        self.reader.list = lambda path: [{"name": "table/_delta_log/00000000000000000002.json"}]
        with self.assertRaisesRegex(RuntimeError, "UnsupportedCheckpoint"):
            self.reader.rows("table")

    def test_transient_timeout_retries_and_returns_actual_response(self):
        with patch.object(self.module.urllib.request, "urlopen",
                          side_effect=[TimeoutError(), io.BytesIO(b"ok")]) as request, \
                patch.object(self.module.time, "sleep"), \
                patch.object(self.module.sys, "stderr", io.StringIO()) as errors:
            response = io.BytesIO(b"ok")
            response.headers = {}
            request.side_effect = [TimeoutError(), response]
            self.assertEqual(self.reader.request("request")[0], b"ok")
            self.assertEqual(request.call_count, 2)
            self.assertIn("independent_read_retry", errors.getvalue())

    def test_persistent_timeout_fails_after_three_attempts(self):
        with patch.object(self.module.urllib.request, "urlopen", side_effect=TimeoutError()) as request, \
                patch.object(self.module.time, "sleep"), \
                patch.object(self.module.sys, "stderr", io.StringIO()):
            with self.assertRaises(TimeoutError):
                self.reader.request("request")
            self.assertEqual(request.call_count, 3)

    def test_request_trace_retains_attempt_timing_and_error_without_credentials_or_query(self):
        response = io.BytesIO(b"ok")
        response.headers = {}
        response.status = 200
        trace = []
        self.reader.trace = trace.append
        request = self.module.urllib.request.Request(
            "https://safe.example/table?continuation=private-query",
            headers={"Authorization": "Bearer private-token"})
        with patch.object(self.module.urllib.request, "urlopen",
                          side_effect=[TimeoutError(), response]), \
                patch.object(self.module.time, "monotonic", side_effect=[0, 0.2, 0.3, 1.2]), \
                patch.object(self.module.time, "sleep"), \
                patch.object(self.module.sys, "stderr", io.StringIO()):
            self.assertEqual(self.reader.request(request)[0], b"ok")
        self.assertEqual(len(trace), 4)
        self.assertEqual(trace[1]["errorCode"], "TimeoutError")
        self.assertEqual(trace[3]["httpStatus"], 200)
        self.assertAlmostEqual(trace[3]["elapsedSeconds"], 0.9)
        self.assertEqual(trace[1]["requestId"], trace[3]["requestId"])
        self.assertEqual({row["path"] for row in trace}, {"/table"})
        self.assertNotIn("private-", json.dumps(trace))

    def test_listing_follows_encoded_continuation_and_keeps_all_pages(self):
        self.reader.token = "test"
        del self.reader.list
        requests = []
        def request(value):
            requests.append(value.full_url)
            page = len(requests)
            return (json.dumps({"paths": [{"name": f"commit{page}"}]}).encode(),
                    {"x-ms-continuation": "a+/= b"} if page == 1 else {})
        self.reader.request = request
        self.assertEqual(self.reader.list("table/_delta_log"),
                         [{"name": "commit1"}, {"name": "commit2"}])
        self.assertIn("continuation=a%2B%2F%3D+b", requests[1])

    def test_repeated_continuation_fails_instead_of_accepting_partial_history(self):
        self.reader.token = "test"
        del self.reader.list
        self.reader.request = lambda request: (b'{"paths":[]}', {"x-ms-continuation": "same"})
        with self.assertRaisesRegex(RuntimeError, "ContinuationRepeated"):
            self.reader.list("table/_delta_log")

    def test_expected_coverage_finishes_only_after_both_tables_or_fails_at_deadline(self):
        for complete in (True, False):
            with self.subTest(complete=complete), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "visibility.jsonl"
                fake = types.SimpleNamespace(
                    close=lambda: None,
                    list=lambda path: [
                        {"name": f"{self.module.LAKEHOUSE}/Tables/{table}", "isDirectory": "true"}
                        for table in ("events", "scores")],
                    rows=lambda path: [{"run_id": "run", "message_id": "message"}] if complete else [])
                with patch.object(self.module, "ProtocolDeltaReader", return_value=fake), \
                        patch.object(self.module.sys, "argv", [
                            "observer", "--run-id", "run", "--output", str(output),
                            "--seconds", "1", "--expected-count", "1"]), \
                        patch.object(self.module.time, "monotonic", side_effect=[0] * 6 + [2]), \
                        patch.object(self.module.time, "sleep"):
                    if complete:
                        self.module.main()
                        self.assertEqual(len(output.read_text().splitlines()), 2)
                        rows = [json.loads(line) for line in output.read_text().splitlines()]
                        self.assertTrue(all("independent_read_started_at" in row for row in rows))
                        self.assertTrue(all("independent_read_finished_at" in row for row in rows))
                        timing_summary = json.loads(output.with_suffix(".timings-summary.json").read_text())
                        self.assertEqual(timing_summary["polls"], 2)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "CoverageIncomplete"):
                            self.module.main()
                        self.assertTrue(output.with_suffix(".timings-summary.json").exists())

    def test_protocol_reader_pins_remote_version_and_uses_datafusion_not_raw_parquet(self):
        with tempfile.TemporaryDirectory() as directory:
            reader = object.__new__(self.module.ProtocolDeltaReader)
            reader.cache = types.SimpleNamespace(name=directory)
            reader.etags = {}
            entries = [
                {"name": "table/_delta_log/00000000000000000007.json", "contentLength": "2", "etag": "log"},
                {"name": "table/deletion_vector_test.bin", "contentLength": "2", "etag": "dv"}]
            reader.list = lambda path, recursive: entries
            fetched = []
            reader.get = lambda name: fetched.append(name) or b"{}"
            library = types.ModuleType("deltalake")
            calls = []
            library.DeltaTable = lambda root, version: calls.append(("version", version)) or types.SimpleNamespace(
                protocol=lambda: types.SimpleNamespace(_asdict=lambda: {"reader_features": ["deletionVectors"]}))
            class Query:
                def register(self, name, table):
                    return self
                def execute(self, sql):
                    calls.append(("sql", sql))
                    return types.SimpleNamespace(read_all=lambda: "dv-applied")
            library.QueryBuilder = Query
            arrow = types.ModuleType("pyarrow")
            arrow.table = lambda batches: types.SimpleNamespace(to_pylist=lambda: [{"logical": batches}])
            with patch.dict(sys.modules, {"deltalake": library, "pyarrow": arrow}):
                self.assertEqual(reader.rows("table"), [{"logical": "dv-applied"}])
                reader.rows("table")
                self.assertEqual(len(fetched), 2)
                self.assertIn(("version", 7), calls)
                self.assertIn(("sql", "SELECT * FROM observed"), calls)
                entries.append({"name": "table/_delta_log/00000000000000000008.json",
                                "contentLength": "2", "etag": "log8"})
                reader.rows("table")
                self.assertIn(("version", 8), calls)
                entries[1]["etag"] = "changed"
                with self.assertRaisesRegex(RuntimeError, "ImmutableFileChanged"):
                    reader.rows("table")

    def test_protocol_reader_rejects_path_escape_and_incomplete_download(self):
        for name, body, code in (
                ("table/../escape", b"{}", "PathUnsafe"),
                ("outside/file", b"{}", "PathOutsideTable"),
                ("table/data.parquet", b"x", "DownloadIncomplete")):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                reader = object.__new__(self.module.ProtocolDeltaReader)
                reader.cache = types.SimpleNamespace(name=directory)
                reader.etags = {}
                reader.list = lambda path, recursive: [
                    {"name": name, "contentLength": "2", "etag": "data"},
                    {"name": "table/_delta_log/00000000000000000000.json",
                     "contentLength": "2", "etag": "log"}]
                reader.get = lambda name: body
                with self.assertRaisesRegex(RuntimeError, code):
                    reader.rows("table")

    def test_real_protocol_engine_reloads_new_version_and_fails_on_missing_data(self):
        import pyarrow as pa
        from deltalake import write_deltalake
        with tempfile.TemporaryDirectory() as remote, tempfile.TemporaryDirectory() as cache:
            source = Path(remote) / "table"
            write_deltalake(str(source), pa.table({"message_id": ["first"]}))
            reader = object.__new__(self.module.ProtocolDeltaReader)
            reader.cache = types.SimpleNamespace(name=cache)
            reader.etags = {}
            trace = []
            reader.trace = trace.append
            reader.list = lambda path, recursive: [
                {"name": "table/" + file.relative_to(source).as_posix(),
                 "contentLength": str(file.stat().st_size), "etag": str(file.stat().st_mtime_ns)}
                for file in source.rglob("*") if file.is_file()]
            reader.get = lambda name: (Path(remote) / name).read_bytes()
            self.assertEqual(reader.rows("table"), [{"message_id": "first"}])
            write_deltalake(str(source), pa.table({"message_id": ["second"]}), mode="append")
            self.assertEqual({row["message_id"] for row in reader.rows("table")}, {"first", "second"})
            self.assertEqual(trace[-1]["remoteVersion"], 1)
            self.assertIn("queryFinishedAtUtc", trace[-1])
            write_deltalake(str(source), pa.table({"message_id": ["third"]}), mode="append")
            original = reader.get
            def missing(name):
                if name.endswith(".parquet"):
                    raise FileNotFoundError("missing-new-file")
                return original(name)
            reader.get = missing
            with self.assertRaises(FileNotFoundError):
                reader.rows("table")
            self.assertEqual(trace[-1]["errorCode"], "FileNotFoundError")
            self.assertNotIn("queryFinishedAtUtc", trace[-1])
            self.assertFalse(any(Path(cache).glob("*-snapshot-2")))

    def test_real_engine_rejects_unsupported_reader_feature_without_publishing_snapshot(self):
        import pyarrow as pa
        from deltalake import write_deltalake
        from deltalake.exceptions import DeltaError
        with tempfile.TemporaryDirectory() as remote, tempfile.TemporaryDirectory() as cache:
            source = Path(remote) / "table"
            write_deltalake(str(source), pa.table({"message_id": ["first"]}))
            log = source / "_delta_log" / "00000000000000000000.json"
            actions = [json.loads(line) for line in log.read_text().splitlines()]
            for action in actions:
                if "protocol" in action:
                    action["protocol"] = {"minReaderVersion": 3, "minWriterVersion": 7,
                                          "readerFeatures": ["unknownReaderFeature"],
                                          "writerFeatures": ["unknownReaderFeature"]}
            log.write_text("\n".join(json.dumps(action) for action in actions))
            reader = object.__new__(self.module.ProtocolDeltaReader)
            reader.cache = types.SimpleNamespace(name=cache)
            reader.etags = {}
            reader.list = lambda path, recursive: [
                {"name": "table/" + file.relative_to(source).as_posix(),
                 "contentLength": str(file.stat().st_size), "etag": str(file.stat().st_mtime_ns)}
                for file in source.rglob("*") if file.is_file()]
            reader.get = lambda name: (Path(remote) / name).read_bytes()
            with self.assertRaises(DeltaError):
                reader.rows("table")
            self.assertFalse(any(Path(cache).glob("*-snapshot-*")))


if __name__ == "__main__":
    unittest.main()
