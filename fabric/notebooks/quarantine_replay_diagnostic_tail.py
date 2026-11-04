"""Explicit single-key quarantine recovery, without new sends or latency claims."""

REPLAY_RUN_ID = "<injected-replay-run-id>"
REPLAY_MESSAGE_ID = "<injected-replay-message-id>"
replay_error = None
try:
    replay_result = replay_quarantined(REPLAY_RUN_ID, REPLAY_MESSAGE_ID)
except Exception as error:
    replay_error = {"errorType": type(error).__name__,
                    "errorCode": str(error) if str(error).startswith("QuarantineReplay") else None}
    replay_result = None

replay_outputs = {
    table: [row.asDict(recursive=True) for row in spark.table(table).where(
        (F.col("run_id") == REPLAY_RUN_ID) & (F.col("message_id") == REPLAY_MESSAGE_ID)).collect()]
    for table in (TABLE_EVENTS, TABLE_SCORES, TABLE_AUDIT, TABLE_QUARANTINE)
}
replay_report = {
    "runId": RUN_ID, "replayTargetRunId": REPLAY_RUN_ID, "replayMessageId": REPLAY_MESSAGE_ID,
    "diagnosticOnly": True, "acceptancePassed": False, "noKafkaSends": True, "noLatencyClaim": True,
    "allDiagnosticStagesSucceeded": replay_error is None,
    "error": replay_error, "replayResult": replay_result,
    "outputRecords": replay_outputs, "stageTimings": stage_timings,
}


def replay_encode(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"UnsupportedReplayEvidenceType:{type(value).__name__}")


replay_json = json.dumps(replay_report, default=replay_encode)
notebookutils.fs.put(f"Files/diagnostics/functional-processing-{RUN_ID}.json", replay_json, True)
notebookutils.notebook.exit(replay_json)
