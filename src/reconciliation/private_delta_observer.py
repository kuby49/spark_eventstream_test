"""Independent committed-Delta visibility reader for bounded VM acceptance captures."""

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

import pyarrow.parquet as pq


HOST = "ddd993fe8d6546f690f57002e6aa5990.zdd.dfs.fabric.microsoft.com"
WORKSPACE = "ddd993fe-8d65-46f6-90f5-7002e6aa5990"
LAKEHOUSE = "500ade3b-e75d-4c1a-b216-2e5930dac64e"
DATABASE = "0f2e913a-7c02-4cde-8d7a-5a8c41aa616b"


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def serialize(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat() if value.tzinfo is None else value.isoformat()
    raise TypeError(type(value).__name__)


class PrivateDeltaReader:
    def __init__(self):
        addresses = {entry[4][0] for entry in socket.getaddrinfo(HOST, 443)}
        if addresses != {"10.74.2.13"}:
            raise RuntimeError("PrivateOneLakeDnsMismatch")
        resource = urllib.parse.quote("https://storage.azure.com", safe="")
        request = urllib.request.Request(
            "http://169.254.169.254/metadata/identity/oauth2/token"
            f"?api-version=2018-02-01&resource={resource}", headers={"Metadata": "true"})
        with urllib.request.urlopen(request, timeout=15) as response:
            self.token = json.load(response)["access_token"]
        self.logs = {}
        self.files = {}
        self.parquet = {}

    def get(self, path):
        request = urllib.request.Request(
            f"https://{HOST}/{WORKSPACE}/{path}",
            headers={"Authorization": f"Bearer {self.token}", "x-ms-version": "2021-06-08"})
        return self.request(request)[0]

    def request(self, request):
        request_id = getattr(self, "request_sequence", 0) + 1
        self.request_sequence = request_id
        trace = getattr(self, "trace", None)
        path = urllib.parse.urlsplit(getattr(request, "full_url", "")).path
        for attempt in range(1, 4):
            started = time.monotonic()
            timing = {"requestId": request_id, "attempt": attempt, "path": path,
                      "startedAtUtc": utcnow()}
            if trace:
                trace(dict(timing, stage="independent_request_started"))
            try:
                with urllib.request.urlopen(request, timeout=10) as response:
                    body, headers = response.read(), response.headers
                    status = getattr(response, "status", None)
            except urllib.error.HTTPError as error:
                code = f"HTTP{error.code}"
                if trace:
                    trace(dict(timing, stage="independent_request_finished",
                               finishedAtUtc=utcnow(), elapsedSeconds=time.monotonic() - started,
                               errorCode=code, httpStatus=error.code))
                if error.code not in {408, 429, 500, 502, 503, 504} or attempt == 3:
                    raise
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                code = type(error).__name__
                if trace:
                    trace(dict(timing, stage="independent_request_finished",
                               finishedAtUtc=utcnow(), elapsedSeconds=time.monotonic() - started,
                               errorCode=code))
                if attempt == 3:
                    raise
            else:
                if trace:
                    trace(dict(timing, stage="independent_request_finished",
                               finishedAtUtc=utcnow(), elapsedSeconds=time.monotonic() - started,
                               httpStatus=status, responseBytes=len(body)))
                return body, headers
            print(json.dumps({"atUtc": utcnow(), "stage": "independent_read_retry",
                              "attempt": attempt, "errorCode": code}), file=sys.stderr, flush=True)
            time.sleep(0.2 * attempt)

    def list(self, directory, recursive=False):
        parameters = {
            "resource": "filesystem", "directory": directory,
            "recursive": str(recursive).lower(), "maxResults": 5000}
        paths = []
        seen_tokens = set()
        for _ in range(1000):
            query = urllib.parse.urlencode(parameters)
            request = urllib.request.Request(
                f"https://{HOST}/{WORKSPACE}?{query}",
                headers={"Authorization": f"Bearer {self.token}", "x-ms-version": "2021-06-08"})
            body, headers = self.request(request)
            paths.extend(json.loads(body).get("paths", []))
            continuation = headers.get("x-ms-continuation")
            if not continuation:
                return paths
            if continuation in seen_tokens:
                raise RuntimeError("DeltaListingContinuationRepeated")
            seen_tokens.add(continuation)
            parameters["continuation"] = continuation
        raise RuntimeError("DeltaListingPageLimitExceeded")

    def rows(self, table_path):
        entries = self.list(f"{table_path}/_delta_log")
        commits = sorted(entry["name"] for entry in entries
                         if entry["name"].rsplit("/", 1)[-1].endswith(".json")
                         and entry["name"].rsplit("/", 1)[-1][:-5].isdigit())
        versions = [int(path.rsplit("/", 1)[-1][:-5]) for path in commits]
        if not versions or versions != list(range(versions[-1] + 1)):
            raise RuntimeError("DeltaHistoryRequiresUnsupportedCheckpoint")
        for path in commits:
            if path in self.logs:
                continue
            actions = [json.loads(line) for line in self.get(path).decode().splitlines() if line]
            active = self.files.setdefault(table_path, {})
            for action in actions:
                if "protocol" in action:
                    protocol = action["protocol"]
                    if (protocol.get("minReaderVersion", 1) not in (1, 3)
                            or set(protocol.get("readerFeatures", [])) - {"deletionVectors"}):
                        raise RuntimeError("UnsupportedDeltaReaderProtocol")
                if "metaData" in action:
                    metadata = action["metaData"]
                    if (metadata.get("partitionColumns")
                            or metadata.get("configuration", {}).get("delta.columnMapping.mode", "none") != "none"):
                        raise RuntimeError("UnsupportedDeltaPartitionOrColumnMapping")
                if "add" in action:
                    add = action["add"]
                    active[add["path"]] = add
                if "remove" in action:
                    active.pop(action["remove"]["path"], None)
            self.logs[path] = True
        rows = []
        for relative, add in self.files[table_path].items():
            # Fabric advertises deletionVectors even before any active file uses one.
            if add.get("deletionVector"):
                raise RuntimeError("UnsupportedDeltaDeletionVector")
            path = f"{table_path}/{relative}"
            if path not in self.parquet:
                data = self.get(path)
                self.parquet[path] = pq.read_table(io.BytesIO(data)).to_pylist()
            rows.extend(self.parquet[path])
        return rows


class ProtocolDeltaReader(PrivateDeltaReader):
    """Private DFS transport; Delta-rs DataFusion interprets the local immutable snapshot."""

    def __init__(self):
        super().__init__()
        self.cache = tempfile.TemporaryDirectory(prefix="esehspark-delta-reader-")
        self.etags = {}

    def rows(self, table_path):
        started = time.monotonic()
        interval = {"stage": "independent_snapshot_finished", "tablePath": table_path,
                    "startedAtUtc": utcnow(), "engine": "delta-rs-datafusion"}
        try:
            return self._rows(table_path, interval)
        except Exception as error:
            interval["errorCode"] = type(error).__name__
            raise
        finally:
            interval.update(finishedAtUtc=utcnow(), elapsedSeconds=time.monotonic() - started)
            trace = getattr(self, "trace", None)
            if trace:
                trace(interval)

    def _rows(self, table_path, interval):
        import pyarrow as pa
        from deltalake import DeltaTable, QueryBuilder

        entries = self.list(table_path, recursive=True)
        interval["discoveryFinishedAtUtc"] = utcnow()
        prefix = table_path + "/"
        files = [entry for entry in entries if entry.get("isDirectory") != "true"]
        versions = [int(entry["name"].rsplit("/", 1)[-1][:-5]) for entry in files
                    if entry["name"].startswith(f"{table_path}/_delta_log/")
                    and entry["name"].rsplit("/", 1)[-1].endswith(".json")
                    and entry["name"].rsplit("/", 1)[-1][:-5].isdigit()]
        if not versions:
            raise RuntimeError("IndependentDeltaVersionMissing")
        version = max(versions)
        interval["remoteVersion"] = version
        root = Path(self.cache.name) / hashlib.sha256(table_path.encode()).hexdigest()
        copied = []
        for entry in files:
            name = entry["name"]
            if not name.startswith(prefix):
                raise RuntimeError("IndependentDeltaPathOutsideTable")
            relative = name[len(prefix):]
            parts = relative.split("/")
            if (not relative or any(part in {"", ".", ".."} for part in parts)
                    or any("\\" in part or ":" in part for part in parts)):
                raise RuntimeError("IndependentDeltaPathUnsafe")
            target = root.joinpath(*parts)
            mutable = relative == "_delta_log/_last_checkpoint"
            if target.exists() and not mutable:
                if (target.stat().st_size != int(entry["contentLength"])
                        or self.etags.get(name) != entry["etag"]):
                    raise RuntimeError("IndependentDeltaImmutableFileChanged")
                copied.append((parts, target))
                continue
            interval["downloadingPath"] = name
            data = self.get(name)
            if len(data) != int(entry["contentLength"]):
                raise RuntimeError("IndependentDeltaDownloadIncomplete")
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(target.name + ".downloading")
            temporary.write_bytes(data)
            temporary.replace(target)
            self.etags[name] = entry["etag"]
            copied.append((parts, target))
        interval.pop("downloadingPath", None)
        interval["downloadsFinishedAtUtc"] = utcnow()
        snapshot = root.parent / f"{root.name}-snapshot-{version}"
        with tempfile.TemporaryDirectory(prefix="snapshot-stage-", dir=self.cache.name) as stage:
            query_root = snapshot if snapshot.exists() else Path(stage) / "table"
            if not snapshot.exists():
                for parts, target in copied:
                    destination = query_root.joinpath(*parts)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    os.link(target, destination)
            # Query only a fully materialized pinned version; never raw physical Parquet rows.
            interval["queryStartedAtUtc"] = utcnow()
            table = DeltaTable(str(query_root), version=version)
            interval["protocol"] = table.protocol()._asdict()
            batches = QueryBuilder().register("observed", table).execute("SELECT * FROM observed")
            rows = pa.table(batches.read_all()).to_pylist()
            interval["queryFinishedAtUtc"] = utcnow()
            if not snapshot.exists():
                query_root.replace(snapshot)
            return rows

    def close(self):
        self.cache.cleanup()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--seconds", type=int, default=240)
    parser.add_argument("--raw", action="store_true")
    parser.add_argument("--expected-count", type=int)
    args = parser.parse_args()
    if args.expected_count is not None and args.expected_count <= 0:
        parser.error("--expected-count must be positive")
    reader = ProtocolDeltaReader()
    output = Path(args.output)
    seen = set()
    if args.raw:
        paths = {"RawEvents": f"{DATABASE}/Tables/RawEvents"}
    else:
        available = reader.list(f"{LAKEHOUSE}/Tables")
        paths = {}
        for table in ("Events", "Scores"):
            matches = [entry["name"] for entry in available
                       if entry["name"].rsplit("/", 1)[-1].casefold() == table.casefold()
                       and entry.get("isDirectory") == "true"]
            if len(matches) != 1:
                raise RuntimeError(f"IndependentTablePathAmbiguous:{table}")
            paths[table] = matches[0]
    timing_path = output.with_suffix(".timings.jsonl")
    summary = {"timingFile": str(timing_path), "requestAttempts": 0, "failedAttempts": 0,
               "polls": 0, "maxRequestSeconds": 0.0, "maxPollSeconds": 0.0}
    try:
        with timing_path.open("a", encoding="utf-8") as timings, output.open("a", encoding="utf-8") as file:
            def trace(entry):
                timings.write(json.dumps(entry) + "\n")
                timings.flush()
                if entry["stage"] == "independent_request_finished":
                    summary["requestAttempts"] += 1
                    summary["failedAttempts"] += int("errorCode" in entry)
                    summary["maxRequestSeconds"] = max(
                        summary["maxRequestSeconds"], entry["elapsedSeconds"])
                elif entry["stage"] == "independent_poll_finished":
                    summary["polls"] += 1
                    summary["maxPollSeconds"] = max(summary["maxPollSeconds"], entry["elapsedSeconds"])

            reader.trace = trace
            # Validate every table before arming; these requests are retained in the timing log too.
            for path in paths.values():
                reader.rows(path)
            output.with_suffix(".ready.json").write_text(json.dumps({
                "armedAtUtc": utcnow(), "runId": args.run_id, "pid": os.getpid(),
                "tables": paths, "independentReader": True, "timingFile": str(timing_path)}), encoding="utf-8")
            deadline = time.monotonic() + args.seconds
            poll = 0
            while time.monotonic() < deadline:
                for table, path in paths.items():
                    poll += 1
                    started = time.monotonic()
                    read_started = utcnow()
                    trace({"stage": "independent_poll_started", "pollId": poll,
                           "table": table, "startedAtUtc": read_started})
                    rows = reader.rows(path)
                    read_finished = utcnow()
                    elapsed = time.monotonic() - started
                    trace({"stage": "independent_poll_finished", "pollId": poll, "table": table,
                           "startedAtUtc": read_started, "finishedAtUtc": read_finished,
                           "elapsedSeconds": elapsed})
                    for row in rows:
                        if row.get("run_id") != args.run_id:
                            continue
                        key = (table, row.get("message_id"))
                        if key in seen:
                            continue
                        seen.add(key)
                        record = dict(row, table=table, independently_read_at=utcnow(),
                                      independent_read_started_at=read_started,
                                      independent_read_finished_at=read_finished,
                                      independent_read_seconds=elapsed, independent_poll_id=poll)
                        file.write(json.dumps(record, default=serialize) + "\n")
                        file.flush()
                        os.fsync(file.fileno())
                if args.expected_count is not None and all(
                        sum(key[0] == table for key in seen) >= args.expected_count for table in paths):
                    return
                time.sleep(0.5)
            if args.expected_count is not None:
                raise RuntimeError("IndependentVisibilityCoverageIncomplete")
    finally:
        output.with_suffix(".timings-summary.json").write_text(json.dumps(summary), encoding="utf-8")
        reader.close()


if __name__ == "__main__":
    main()
