"""Bounded processor-boundary faults in the existing private Spark environment.

Inputs below are explicitly synthetic, not Kafka deliveries or latency samples.
KQL, successful scoring, Delta persistence and replay use the real dependencies.
"""

import copy
import io

fault_cases = []
fault_inputs = []
fault_original_query = _query_kusto
fault_original_api = _call_score_api
fault_original_upsert = _upsert
fault_original_urlopen = urllib.request.urlopen
fault_original_key = FUNCTION_KEY
fault_api_calls = []
fault_http_statuses = []
fault_kql_errors = []


def fault_observe_http(request, *args, **kwargs):
    try:
        response = fault_original_urlopen(request, *args, **kwargs)
    except urllib.error.HTTPError as error:
        if request.full_url == f"{KUSTO_ENDPOINT.rstrip('/')}/v1/rest/query":
            body = json.loads(error.read().decode("utf-8"))
            detail = body.get("error", {})
            fault_kql_errors.append({
                "status": error.code, "code": detail.get("code"),
                "type": detail.get("@type"),
                "clientRequestId": request.get_header("X-ms-client-request-id"),
                "serviceRequestId": error.headers.get("x-ms-request-id"),
            })
        raise
    if request.full_url == API_URL:
        payload = json.loads(request.data)
        fault_http_statuses.append({
            "run_id": payload["run_id"], "message_id": payload["message_id"],
            "status": response.status})
    return response


def fault_api(event, calculated):
    score, attempts = fault_original_api(event, calculated)
    fault_api_calls.append({"run_id": event["run_id"], "message_id": event["message_id"],
                            "score": score, "attempts": attempts})
    return score, attempts


_call_score_api = fault_api


def fault_fixture():
    event = {"run_id": str(uuid.uuid4()), "message_id": str(uuid.uuid4()),
             "equipment_id": 17, "temperature": 95.0,
             "timestamp": "2000-01-01T00:20:00Z"}
    row = {"parsed": event, "value": json.dumps(event, sort_keys=True, separators=(",", ":")),
           "topic": "explicit-synthetic-processor-fixture", "partition": 0,
           "offset": len(fault_inputs), "timestamp": _utcnow(), "envelope_index": 0}
    fault_inputs.append(copy.deepcopy(row))
    return row


def fault_records(row, table):
    return [entry.asDict(recursive=True) for entry in spark.table(table).where(
        (F.col("run_id") == row["parsed"]["run_id"])
        & (F.col("message_id") == row["parsed"]["message_id"])).collect()]


def require(condition, code):
    if not condition:
        raise RuntimeError(code)


def fault_case(name, operation, scope):
    started = time.monotonic()
    urllib.request.urlopen = fault_observe_http
    try:
        detail = operation()
        fault_cases.append({"name": name, "status": "Succeeded", "scope": scope, "detail": detail})
    except Exception as error:
        fault_cases.append({"name": name, "status": "Failed", "scope": scope,
                            "errorType": type(error).__name__,
                            "errorCode": str(error) if isinstance(error, (KqlQueryError, ApiRequestError))
                            or str(error).startswith("FaultAssertion") else None})
    finally:
        globals()["_query_kusto"] = fault_original_query
        globals()["_upsert"] = fault_original_upsert
        urllib.request.urlopen = fault_original_urlopen
        globals()["FUNCTION_KEY"] = fault_original_key
        fault_cases[-1]["elapsedSeconds"] = time.monotonic() - started


def successful_empty():
    row = fault_fixture()
    process_records([row, copy.deepcopy(row)])
    process_records([copy.deepcopy(row)])
    transport_repeat = copy.deepcopy(row)
    transport_repeat["parsed"].update(
        EventEnqueuedUtcTime="2000-01-01T00:21:00Z",
        EventProcessedUtcTime="2000-01-01T00:22:00Z", PartitionId=3)
    transport_repeat["value"] = json.dumps(transport_repeat["parsed"], indent=2)
    process_records([transport_repeat])
    event = fault_records(row, TABLE_EVENTS)
    scores = fault_records(row, TABLE_SCORES)
    audit = fault_records(row, TABLE_AUDIT)
    calls = [call for call in fault_api_calls if call["message_id"] == row["parsed"]["message_id"]]
    require(len(event) == len(scores) == len(audit) == len(calls) == 1, "FaultAssertionDuplicateReplay")
    require(type(scores[0]["score"]) is int and 1 <= scores[0]["score"] <= 100,
            "FaultAssertionRandomScoreRange")
    require(event[0]["calculated_temperature"] == 95.0 and
            audit[0]["fallback_reason"] == "SuccessfulEmptyOrNullKqlResult" and
            audit[0]["query_unique_count"] == 0 and audit[0]["raw_visible_at"] is None,
            "FaultAssertionSuccessfulEmptyFallback")
    conflict = copy.deepcopy(row)
    conflict["parsed"]["temperature"] = 96.0
    conflict["value"] = json.dumps(conflict["parsed"], sort_keys=True, separators=(",", ":"))
    process_records([conflict])
    require(fault_records(row, TABLE_QUARANTINE)[0]["stage"] == "IDENTITY" and
            fault_records(row, TABLE_AUDIT)[0]["status"] == "SCORED" and
            fault_records(row, TABLE_SCORES)[0]["score"] == scores[0]["score"],
            "FaultAssertionConflictChangedWinner")
    return {"runId": row["parsed"]["run_id"], "messageId": row["parsed"]["message_id"],
            "logicalScores": 1, "successfulApiCalls": 1, "transportMetadataReplaySkipped": True,
            "conflictQuarantined": True}


def failed_query():
    row = fault_fixture()
    def reject(query, parameters, **kwargs):
        # A read-only invalid projection returns a real service query error.
        return fault_original_query("print fault = esehspark_missing_fault_identifier", {})
    globals()["_query_kusto"] = reject
    process_records([row])
    require(not fault_records(row, TABLE_EVENTS) and not fault_records(row, TABLE_SCORES)
            and fault_records(row, TABLE_QUARANTINE)[0]["reason"] == "KQLHttp400",
            "FaultAssertionQueryErrorBecameFallback")
    return {"runId": row["parsed"]["run_id"], "messageId": row["parsed"]["message_id"],
            "queryOverride": "ReadOnlyInvalidProjection", "actualServiceError": "KQLHttp400",
            "fallbackUsed": False, "errorEvidence": fault_kql_errors[-1]}


def failed_api(kind):
    row = fault_fixture()
    attempts = []
    def inject(request, *args, **kwargs):
        if request.full_url != API_URL:
            return fault_original_urlopen(request, *args, **kwargs)
        attempts.append(time.monotonic())
        if kind == "timeout":
            raise TimeoutError()
        if kind == "503":
            raise urllib.error.HTTPError(API_URL, 503, "Injected", {}, io.BytesIO())
        return io.BytesIO(json.dumps({
            "run_id": row["parsed"]["run_id"], "message_id": row["parsed"]["message_id"],
            "score": 101}).encode())
    urllib.request.urlopen = inject
    process_records([row])
    expected_attempts = 1 if kind == "invalid" else 2
    quarantine = fault_records(row, TABLE_QUARANTINE)
    require(len(fault_records(row, TABLE_EVENTS)) == 1 and not fault_records(row, TABLE_SCORES)
            and len(quarantine) == 1 and quarantine[0]["stage"] == "API"
            and len(attempts) == expected_attempts,
            "FaultAssertionApiFailureLostEventOrScored")
    detail = {"runId": row["parsed"]["run_id"], "messageId": row["parsed"]["message_id"],
              "injected": kind, "attempts": len(attempts), "reason": quarantine[0]["reason"]}
    if kind == "503":
        urllib.request.urlopen = fault_observe_http
        replay_quarantined(row["parsed"]["run_id"], row["parsed"]["message_id"])
        resolved = fault_records(row, TABLE_QUARANTINE)[0]
        require(json.loads(resolved["resolution"])["status"] == "RESOLVED"
                and len(fault_records(row, TABLE_SCORES)) == 1
                and fault_records(row, TABLE_AUDIT)[0]["status"] == "SCORED",
                "FaultAssertionQuarantineReplayUnresolved")
        detail["explicitReplay"] = "ResolvedWithOriginalIdentity"
    return detail


def restart_after_api():
    row = fault_fixture()
    def fail_score(table, records):
        if table == TABLE_SCORES:
            raise RuntimeError("InjectedScorePersistenceFailure")
        return fault_original_upsert(table, records)
    globals()["_upsert"] = fail_score
    try:
        process_records([row])
        raise RuntimeError("FaultAssertionStorageErrorWasSwallowed")
    except RuntimeError as error:
        if str(error) != "InjectedScorePersistenceFailure":
            raise
    require(len(fault_records(row, TABLE_EVENTS)) == 1 and not fault_records(row, TABLE_SCORES),
            "FaultAssertionPartialStageShape")
    globals()["_upsert"] = fault_original_upsert
    process_records([copy.deepcopy(row)])
    calls = [call for call in fault_api_calls if call["message_id"] == row["parsed"]["message_id"]]
    statuses = [entry["status"] for entry in fault_http_statuses
                if entry["message_id"] == row["parsed"]["message_id"]]
    require(len(calls) == 2 and calls[0]["score"] == calls[1]["score"]
            and statuses == [201, 200]
            and len(fault_records(row, TABLE_SCORES)) == 1
            and fault_records(row, TABLE_AUDIT)[0]["status"] == "SCORED",
            "FaultAssertionDurableApiReplayDiverged")
    return {"runId": row["parsed"]["run_id"], "messageId": row["parsed"]["message_id"],
            "httpInvocations": 2, "httpStatuses": statuses,
            "oneStoredScoreReused": True, "score": calls[0]["score"]}


try:
    fault_case("warm-dependencies", lambda: {
        "allocation": _verify_session_allocation(),
        "kql": warm_kql_transport(), "delta": warm_output_writes()},
        "Read-only KQL and unchanged retained-row MERGEs; no synthetic event yet")
    if fault_cases[-1]["status"] == "Succeeded":
        fault_case("successful-empty-duplicate-replay-conflict", successful_empty,
                   "Synthetic inputs; real empty KQL snapshot, private API and Delta")
        fault_case("query-error-is-not-empty", failed_query,
                   "Read-only invalid query override; actual KQL HTTP400 and real audit/quarantine persistence")
        for kind in ("timeout", "503", "invalid"):
            fault_case(f"api-{kind}-preserves-events", lambda kind=kind: failed_api(kind),
                       "Injected urllib API response/transport; real Events/audit/quarantine")
        fault_case("restart-after-api-before-scores", restart_after_api,
                   "Injected Delta write failure; real KQL/API/Delta and durable API replay")
finally:
    _query_kusto = fault_original_query
    _call_score_api = fault_original_api
    _upsert = fault_original_upsert
    urllib.request.urlopen = fault_original_urlopen
    FUNCTION_KEY = fault_original_key

fault_run_ids = [row["parsed"]["run_id"] for row in fault_inputs]
fault_report = {
    "runId": RUN_ID, "diagnosticOnly": True, "faultMatrix": True, "acceptancePassed": False,
    "allDiagnosticStagesSucceeded": bool(fault_cases) and all(
        case["status"] == "Succeeded" for case in fault_cases),
    "noKafkaSends": True, "noLatencyClaim": True, "cases": fault_cases,
    "fixtures": fault_inputs, "apiCalls": fault_api_calls,
    "apiHttpStatuses": fault_http_statuses, "stageTimings": stage_timings,
    "kqlServiceErrors": fault_kql_errors,
    "outputRecords": {table: [row.asDict(recursive=True) for row in spark.table(table).where(
        F.col("run_id").isin(fault_run_ids)).collect()]
        for table in (TABLE_EVENTS, TABLE_SCORES, TABLE_AUDIT, TABLE_QUARANTINE)},
}
def fault_encode(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"UnsupportedFaultEvidenceType:{type(value).__name__}")


fault_json = json.dumps(fault_report, default=fault_encode)
notebookutils.fs.put(f"Files/diagnostics/functional-processing-{RUN_ID}.json", fault_json, True)
notebookutils.notebook.exit(fault_json)
