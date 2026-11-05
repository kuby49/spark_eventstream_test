"""Bounded baseline using production helpers and an isolated Kafka checkpoint."""

import traceback

baseline_batches = []
baseline_derived = []
baseline_progress = []
baseline_progress_keys = set()
baseline_error = None
baseline_start = datetime.now(timezone.utc)
active = None
LOAD_TEST = "<injected-load-test>" == "true"
STRESS_TEST = "<injected-stress-test>" == "true"
PREVIOUS_STRESS_RUN_ID = "<injected-previous-stress-run-id>"
SUSTAINED_DIAGNOSTIC = "<injected-sustained-diagnostic>" == "true"
TEST_RATE = int("<injected-test-rate>")
TEST_MINUTES = int("<injected-test-minutes>")
EXPECTED_HIGH = TEST_RATE * TEST_MINUTES // 10
optimization_results = []
post_step_optimization = {"status": "NotRequested", "results": []}
warm_kafka_metadata = None
consumer_warmup = None
kql_warmup = None
session_allocation = None
stress_health = []
stress_health_errors = []
stress_last_sample = 0.0
stress_last_offsets = 0.0
stress_broker = None
stress_scaling_events = []
stress_previous_executors = None


def stress_sample(progress):
    global stress_last_sample, stress_last_offsets, stress_broker, stress_previous_executors
    if not STRESS_TEST or time.monotonic() - stress_last_sample < 15:
        return
    stress_last_sample = time.monotonic()
    allocation = _verify_session_allocation()
    executors = allocation["activeExecutors"]
    if stress_previous_executors is not None and executors != stress_previous_executors:
        stress_scaling_events.append({
            "observedAtUtc": allocation["checkedAtUtc"],
            "fromExecutors": stress_previous_executors, "toExecutors": executors,
            "direction": "up" if executors > stress_previous_executors else "down",
            "observedDriverAndExecutorCores": allocation["observedDriverAndExecutorCores"],
        })
    stress_previous_executors = executors
    if time.monotonic() - stress_last_offsets >= 60:
        stress_broker = broker_metadata()
        stress_last_offsets = time.monotonic()
    offsets = (progress or {}).get("sources", [{}])[0].get("endOffset", {}).get(KAFKA_TOPIC, {})
    lag = [{"partition": row["partition"], "end": row["end"],
            "committed": offsets.get(str(row["partition"])),
            "lagEnvelopes": (max(0, row["end"] - offsets[str(row["partition"])]))
            if str(row["partition"]) in offsets else None}
           for row in stress_broker["partitionOffsets"]]
    sample = {
        "runId": RUN_ID, "atUtc": datetime.now(timezone.utc).isoformat(),
        "consumerActive": active.isActive,
        "completedLogicalEvents": sum(batch["logicalEvents"] for batch in baseline_batches),
        "observedLogicalEvents": len(baseline_derived), "partitionLag": lag,
        "lastBatchFinishedAtUtc": baseline_batches[-1]["finishedAtUtc"] if baseline_batches else None,
        "progress": progress,
        "allocation": allocation, "scalingEvents": list(stress_scaling_events),
        "rejections": [{"stage": timing["stage"], "errorCode": timing.get("errorCode"),
                        "errorType": timing.get("errorType")}
                       for timing in stage_timings if timing.get("errorType")],
    }
    stress_health.append(sample)
    notebookutils.fs.put(f"Files/diagnostics/stress-health-{RUN_ID}.json",
                        json.dumps(sample), True)


def baseline_progress_dict(progress):
    if progress is None:
        return None
    return json.loads(progress.json) if hasattr(progress, "json") else dict(progress)


def baseline_batch(batch, batch_id):
    started = time.monotonic()
    logical = 0
    records = []
    for row in batch.select(
            "topic", "partition", "offset", "timestamp",
            F.col("value").cast("string").alias("value")).toLocalIterator():
        for event_row in decode_kafka_envelope(row.asDict(recursive=True)):
            event = event_row.get("parsed")
            if isinstance(event, dict) and event.get("run_id") != RUN_ID:
                continue
            baseline_derived.append({
                "topic": event_row["topic"], "partition": event_row["partition"],
                "offset": event_row["offset"], "envelope_index": event_row["envelope_index"],
                "event": event, "observedAtUtc": datetime.now(timezone.utc).isoformat(),
            })
            logical += 1
            records.append(event_row)
    read_finished = time.monotonic()
    process_records(records)
    baseline_batches.append({"batchId": batch_id, "logicalEvents": logical,
                             "transportReadSeconds": read_finished - started,
                             "processingSeconds": time.monotonic() - read_finished,
                             "elapsedSeconds": time.monotonic() - started,
                             "finishedAtUtc": datetime.now(timezone.utc).isoformat()})


try:
    session_allocation = _verify_session_allocation()
    kql_warmup = warm_kql_transport()
    if LOAD_TEST and not STRESS_TEST:
        for table in (TABLE_EVENTS, TABLE_SCORES):
            optimized_at = datetime.now(timezone.utc).isoformat()
            metrics = [row.asDict(recursive=True) for row in spark.sql(f"OPTIMIZE {table}").collect()]
            optimization_results.append({"table": table, "phase": "before_step",
                                         "postStepForRunId": PREVIOUS_STRESS_RUN_ID or None,
                                         "startedAtUtc": optimized_at,
                                         "finishedAtUtc": datetime.now(timezone.utc).isoformat(),
                                         "metrics": metrics})
    warm_output_writes()
    warm_kafka_metadata = broker_metadata()
    warm_ends = {str(item["partition"]): item["end"]
                 for item in warm_kafka_metadata["partitionOffsets"]}
    warm_starts = {str(item["partition"]): max(item["beginning"], item["end"] - 1)
                   for item in warm_kafka_metadata["partitionOffsets"]}
    baseline_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.group.id", f"esehspark-baseline-{RUN_ID}")
        .option("startingOffsets", json.dumps({KAFKA_TOPIC: warm_starts}))
        .option("failOnDataLoss", "true")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                'username="$ConnectionString" '
                f'password="{KAFKA_CONNECTION}";')
        .load()
    )
    active = (baseline_stream.writeStream.foreachBatch(baseline_batch)
              .option("checkpointLocation", CHECKPOINT)
              .trigger(processingTime="1 second").start())
    deadline = time.monotonic() + TEST_MINUTES * 60 + 600
    armed = False
    while time.monotonic() < deadline:
        active.awaitTermination(5)
        if active.exception():
            raise active.exception()
        if not active.isActive:
            raise RuntimeError("BaselineStreamTerminated")
        for item in active.recentProgress:
            item = baseline_progress_dict(item)
            progress_key = (item.get("batchId"), item.get("timestamp"))
            if progress_key not in baseline_progress_keys:
                baseline_progress_keys.add(progress_key)
                baseline_progress.append(item)
        progress = baseline_progress_dict(active.lastProgress)
        stress_sample(progress)
        caught_up = (progress is not None and all(
            progress["sources"][0]["endOffset"][KAFKA_TOPIC].get(partition, -1) >= end
            for partition, end in warm_ends.items()))
        if not armed and caught_up:
            session_allocation = _verify_session_allocation(require_executor=True)
            consumer_warmup = {
                "retainedStartOffsets": warm_starts, "capturedEndOffsets": warm_ends,
                "completedProgress": progress, "newEventsSent": 0,
                "purpose": "Fetch one retained envelope per nonempty partition before measured sends"}
            notebookutils.fs.put(
                f"Files/diagnostics/functional-ready-{RUN_ID}.json",
                json.dumps({"runId": RUN_ID, "consumerActive": True,
                            "consumerWarmup": consumer_warmup,
                            "sessionAllocation": session_allocation,
                            "fixtureRunId": RUN_ID, "fixtureMessageId": FIXTURE_MESSAGE_ID}), True)
            armed = True
        completed_events = sum(batch["logicalEvents"] for batch in baseline_batches)
        last_batch_committed = (bool(baseline_batches) and progress is not None
                                and int(progress["batchId"]) >= baseline_batches[-1]["batchId"])
        if (len(baseline_derived) >= EXPECTED_HIGH and completed_events == len(baseline_derived)
                and last_batch_committed):
            audit = spark.table(TABLE_AUDIT).where(F.col("run_id") == RUN_ID).collect()
            if len([row for row in audit if row["status"] in (
                    "SCORED", "API_FAILED", "QUARANTINED")]) >= EXPECTED_HIGH:
                raw_count = _rows_from_kusto(_query_kusto(
                    "declare query_parameters(runId:string); RawEvents | where run_id == runId "
                    "| summarize physical=count()", {"runId": RUN_ID}), ["physical"])
                # The last high event may precede the final low events in the last minute.
                if len(raw_count) == 1 and int(raw_count[0][0]) >= TEST_RATE * TEST_MINUTES:
                    break
    else:
        raise TimeoutError("BaselineDrainDeadlineExceeded")
except Exception as error:
    baseline_error = {"errorType": type(error).__name__,
                      "errorCode": str(error) if isinstance(error, KqlQueryError) else None,
                      "diagnosticCodes": sorted(set(re.findall(
                          r"\b(?:[A-Za-z]*OutOfMemory[A-Za-z]*|ExecutorLostFailure|"
                          r"FetchFailed|KQLHttp[0-9]{3}|Baseline[A-Za-z]+|"
                          r"FixedExecutor[A-Za-z]+|[A-Za-z]*Throttl[A-Za-z]*)\b", str(error)))),
                      "traceback": [{"function": frame.name, "line": frame.lineno}
                                    for frame in traceback.extract_tb(error.__traceback__)]}
finally:
    if active is not None:
        active.stop()

if STRESS_TEST:
    post_step_optimization["consumerStoppedAtUtc"] = datetime.now(timezone.utc).isoformat()
    if baseline_error is not None:
        post_step_optimization["status"] = "SkippedAfterProcessingFailure"
    else:
        try:
            if active is not None and active.isActive:
                raise RuntimeError("OptimizeBlockedActiveConsumer")
            for table in (TABLE_EVENTS, TABLE_SCORES, TABLE_AUDIT, TABLE_QUARANTINE):
                optimized_at = datetime.now(timezone.utc).isoformat()
                metrics = [row.asDict(recursive=True)
                           for row in spark.sql(f"OPTIMIZE {table}").collect()]
                post_step_optimization["results"].append({
                    "table": table, "phase": "after_step",
                    "startedAtUtc": optimized_at,
                    "finishedAtUtc": datetime.now(timezone.utc).isoformat(),
                    "metrics": metrics, "allocation": _verify_session_allocation(),
                })
            post_step_optimization["status"] = "Completed"
        except Exception as error:
            post_step_optimization["status"] = "Failed"
            post_step_optimization["error"] = {
                "errorType": type(error).__name__,
                "traceback": [{"function": frame.name, "line": frame.lineno}
                              for frame in traceback.extract_tb(error.__traceback__)],
            }

baseline_outputs = {
    table: [row.asDict(recursive=True) for row in spark.table(table)
            .where(F.col("run_id") == RUN_ID).collect()]
    for table in (TABLE_EVENTS, TABLE_SCORES, TABLE_AUDIT, TABLE_QUARANTINE)
}
raw_columns = ["run_id", "message_id", "equipment_id", "temperature", "timestamp"]
raw_rows = []
raw_window_rows = []
try:
    raw_response = _query_kusto(
        "declare query_parameters(runId:string); RawEvents | where run_id == runId "
        "| project run_id, message_id, equipment_id, temperature, timestamp",
        {"runId": RUN_ID})
    raw_rows = [dict(zip(raw_columns, row))
                for row in _rows_from_kusto(raw_response, raw_columns)]
    window_response = _query_kusto(
        "declare query_parameters(runId:string); "
        "let Bounds=RawEvents | where run_id == runId "
        "| summarize WindowStart=min(timestamp), WindowEnd=max(timestamp); "
        "RawEvents | where timestamp between "
        "(toscalar(Bounds | project WindowStart) - 20m .. toscalar(Bounds | project WindowEnd)) "
        "| project run_id, message_id, equipment_id, temperature, timestamp",
        {"runId": RUN_ID})
    raw_window_rows = [dict(zip(raw_columns, row))
                       for row in _rows_from_kusto(window_response, raw_columns)]
except Exception as error:
    baseline_error = {"stage": "raw_evidence", "errorType": type(error).__name__,
                      "errorCode": str(error) if isinstance(error, KqlQueryError) else None,
                      "priorError": baseline_error}
baseline_report = {
    "runId": RUN_ID, "baselineRun": not LOAD_TEST and not SUSTAINED_DIAGNOSTIC,
    "loadTest": LOAD_TEST, "sustainedDiagnostic": SUSTAINED_DIAGNOSTIC,
    "rate": TEST_RATE, "minutes": TEST_MINUTES, "optimizations": optimization_results,
    "postStepOptimization": post_step_optimization,
    "acceptancePassed": False,
    "oapEnabled": False, "startedAtUtc": baseline_start.isoformat(),
    "finishedAtUtc": datetime.now(timezone.utc).isoformat(),
    "error": baseline_error, "outputRecords": baseline_outputs, "rawRecords": raw_rows,
    "rawWindowRecords": raw_window_rows,
    "derivedRecords": baseline_derived, "batches": baseline_batches,
    "progress": baseline_progress, "stageTimings": stage_timings,
    "kafkaWarmup": consumer_warmup, "brokerMetadataBefore": warm_kafka_metadata,
    "kqlWarmup": kql_warmup,
    "sessionAllocation": session_allocation,
    "stressTest": STRESS_TEST, "stressHealth": stress_health,
    "stressHealthErrors": stress_health_errors,
    "scalingEvents": stress_scaling_events,
    "allocationBasis": "dynamic_1_to_5_medium_existing_pool",
}


def baseline_encode(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat() if value.tzinfo is None else value.isoformat()
    raise TypeError(type(value).__name__)


baseline_json = json.dumps(baseline_report, default=baseline_encode)
baseline_evidence_path = f"Files/diagnostics/functional-processing-{RUN_ID}.json"
if STRESS_TEST:
    reporting_phases = []

    def persist_reporting_phase(phase):
        reporting_phases.append({
            "phase": phase, "atUtc": datetime.now(timezone.utc).isoformat(),
        })
        notebookutils.fs.put(
            f"Files/diagnostics/reporting-phases-{RUN_ID}.json",
            json.dumps({
                "runId": RUN_ID, "evidencePath": baseline_evidence_path,
                "evidenceBytes": len(baseline_json.encode("utf-8")),
                "phases": reporting_phases,
                "exitReturnMarkerExpected": False,
            }), True)

    persist_reporting_phase("before_final_evidence_write")
    notebookutils.fs.put(baseline_evidence_path, baseline_json, True)
    persist_reporting_phase("after_final_evidence_write")
    baseline_exit = json.dumps({
        "kind": "persisted_stress_evidence_v1", "runId": RUN_ID,
        "evidencePath": baseline_evidence_path,
    })
    persist_reporting_phase("before_notebook_exit")
    notebookutils.notebook.exit(baseline_exit)
else:
    notebookutils.fs.put(baseline_evidence_path, baseline_json, True)
    notebookutils.notebook.exit(baseline_json)
