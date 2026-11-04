"""Appended to production processing helpers for isolated OAP-off fixture verification."""

import re

FIXTURE_RUN_ID = "<injected-fixture-run-id>"
FIXTURE_MESSAGE_ID = "<injected-fixture-message-id>"
EXPECTED_AVERAGE = float("<injected-fixture-average>")
LIVE_FIXTURE = "<injected-live-fixture>" == "true"
gate_results = []
consumer_observations = []
stage_timings = []


def profile_helper(name, operation):
    def profiled(*args, **kwargs):
        started = time.monotonic()
        timing = {"stage": name, "startedAtUtc": datetime.now(timezone.utc).isoformat()}
        if name == "delta_merge":
            timing["table"] = args[0]
        if name == "score_api":
            timing["runId"] = args[0]["run_id"]
            timing["messageId"] = args[0]["message_id"]
        try:
            result = operation(*args, **kwargs)
            if name == "score_api":
                timing["attempts"] = result[1]
            if name == "kql_request":
                for table in result.get("Tables", result.get("tables", [])):
                    columns = [column.get("ColumnName", column.get("columnName")) for column
                               in table.get("Columns", table.get("columns", []))]
                    if columns == ["average", "unique_count", "trigger_visible", "window_records"]:
                        timing["aggregateProjection"] = columns
                        timing["aggregateRows"] = table.get("Rows", table.get("rows"))
                        timing["parameters"] = args[1]
            return result
        except Exception as error:
            timing["errorType"] = type(error).__name__
            if isinstance(error, KqlQueryError):
                timing["errorCode"] = str(error)
                timing["attempts"] = error.attempts
                timing["errorCause"] = error.cause
            raise
        finally:
            timing["elapsedSeconds"] = time.monotonic() - started
            timing["finishedAtUtc"] = datetime.now(timezone.utc).isoformat()
            stage_timings.append(timing)
    return profiled


_query_kusto = profile_helper("kql_request", _query_kusto)
_eventhouse_average = profile_helper("kql_visibility_and_average", _eventhouse_average)
_audit_for = profile_helper("audit_read", _audit_for)
_upsert = profile_helper("delta_merge", _upsert)
_call_score_api = profile_helper("score_api", _call_score_api)


def warm_output_writes():
    warmed = []
    for table in (TABLE_EVENTS, TABLE_SCORES, TABLE_AUDIT):
        rows = spark.table(table).where(
            F.col("message_id").isNotNull() & F.col("run_id").isNotNull()).limit(1).collect()
        if not rows:
            raise RuntimeError("WarmOutputFixtureMissing")
        # Re-MERGE an unchanged retained row; do not generate or score another event.
        _upsert(table, [rows[0].asDict(recursive=True)])
        warmed.append(table)
    return {"unchangedRetainedRows": warmed}


def warm_kql_transport():
    started = time.monotonic()
    for attempt in (1, 2, 3):
        try:
            response = _query_kusto("print ready=1", {})
        except KqlQueryError as error:
            if attempt == 3 or str(error) not in {
                    "KQLTransportError", "KQLDeadlineExceeded", "KQLHttp408", "KQLHttp429",
                    "KQLHttp500", "KQLHttp502", "KQLHttp503", "KQLHttp504"}:
                raise
            time.sleep(1)
            continue
        if _rows_from_kusto(response, ["ready"]) != [[1]]:
            raise RuntimeError("WarmKqlResponseMismatch")
        return {"readOnlyQuery": True, "warmupAttempts": attempt,
                "elapsedSeconds": time.monotonic() - started,
                "scope": "Before measured sends; per-event query budget is unchanged"}


def gate_stage(name, operation):
    try:
        detail = operation()
        gate_results.append({"stage": name, "status": "Succeeded", "detail": detail})
        return True
    except Exception as error:
        gate_results.append({
            "stage": name, "status": "Failed", "errorType": type(error).__name__,
            "errorCodes": sorted(set(re.findall(
                r"\b(?:UnknownHostException|TimeoutException|SaslAuthenticationException|"
                r"TopicAuthorizationException|SSLHandshakeException|ForbiddenByRbac|"
                r"FunctionalOutputMismatch|KQLHttp[0-9]{3}|RawEventVisibilityTimeout|"
                r"FunctionalStreamNoProgress|FunctionalStreamTerminated)\b", str(error)))),
        })
        return False


def broker_metadata():
    properties = spark._jvm.java.util.Properties()
    settings = {
        "bootstrap.servers": KAFKA_BOOTSTRAP, "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN", "request.timeout.ms": "15000",
        "default.api.timeout.ms": "20000",
        "group.id": f"esehspark-offsets-{RUN_ID}",
        "key.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "value.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "sasl.jaas.config":
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            'username="$ConnectionString" '
            f'password="{KAFKA_CONNECTION.replace(chr(92), chr(92)*2).replace(chr(34), chr(92)+chr(34))}";',
    }
    for key, value in settings.items():
        properties.setProperty(key, value)
    client = spark._jvm.org.apache.kafka.clients.admin.AdminClient.create(properties)
    try:
        nodes = client.describeCluster().nodes().get(
            20, spark._jvm.java.util.concurrent.TimeUnit.SECONDS).iterator()
        observed = []
        while nodes.hasNext():
            node = nodes.next()
            addresses = sorted({entry[4][0] for entry in socket.getaddrinfo(
                node.host(), node.port(), type=socket.SOCK_STREAM)})
            observed.append({"id": node.id(), "host": node.host(), "port": node.port(),
                             "resolvedAddresses": addresses,
                             "allAddressesPrivate": bool(addresses) and all(
                                 ipaddress.ip_address(address).is_private for address in addresses)})
        if not observed:
            raise RuntimeError("NoAdvertisedKafkaBrokers")
        consumer = spark._jvm.org.apache.kafka.clients.consumer.KafkaConsumer(properties)
        try:
            partitions = consumer.partitionsFor(KAFKA_TOPIC, spark._jvm.java.time.Duration.ofSeconds(20))
            topics = spark._jvm.java.util.ArrayList()
            iterator = partitions.iterator()
            while iterator.hasNext():
                partition = iterator.next()
                topics.add(spark._jvm.org.apache.kafka.common.TopicPartition(KAFKA_TOPIC, partition.partition()))
            beginnings = consumer.beginningOffsets(topics, spark._jvm.java.time.Duration.ofSeconds(20))
            ends = consumer.endOffsets(topics, spark._jvm.java.time.Duration.ofSeconds(20))
            offsets = []
            iterator = topics.iterator()
            while iterator.hasNext():
                partition = iterator.next()
                offsets.append({"partition": int(partition.partition()),
                                "beginning": int(beginnings.get(partition)),
                                "end": int(ends.get(partition))})
        finally:
            consumer.close(spark._jvm.java.time.Duration.ofSeconds(5))
        return {"advertisedBrokers": observed, "topic": KAFKA_TOPIC, "partitionOffsets": offsets,
                "scope": "Advertised metadata, offsets and DNS, not per-broker traffic capture"}
    finally:
        client.close(spark._jvm.java.time.Duration.ofSeconds(5))


def parameterized_kql():
    response = _query_kusto(
        "declare query_parameters(runId:string, messageId:string); "
        "RawEvents | where run_id == runId and message_id == messageId | summarize visible=count()",
        {"runId": FIXTURE_RUN_ID, "messageId": FIXTURE_MESSAGE_ID})
    rows = _rows_from_kusto(response, ["visible"])
    if len(rows) != 1 or rows[0][0] != 1:
        raise RuntimeError("FunctionalOutputMismatch")
    return {"matchingRawHighRows": rows[0][0], "executedFrom": "Spark"}


def process_fixture():
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.group.id", f"esehspark-functional-{RUN_ID}")
        .option("startingOffsets", "latest" if LIVE_FIXTURE else "earliest")
        .option("maxOffsetsPerTrigger", "10")
        .option("kafka.security.protocol", "SASL_SSL").option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                'username="$ConnectionString" '
                f'password="{KAFKA_CONNECTION.replace(chr(92), chr(92)*2).replace(chr(34), chr(92)+chr(34))}";')
        .load().select("topic", "partition", "offset", "timestamp", "value")
    )
    def observed_batch(batch, batch_id):
        samples = batch.select("partition", "offset", F.col("value").cast("string").alias("body")).limit(10).collect()
        observed = []
        for row in samples:
            try:
                body = json.loads(row.body)
                entries = body if isinstance(body, list) else [body]
                observed.append({
                    "partition": row.partition, "offset": row.offset,
                    "jsonType": type(body).__name__,
                    "keys": sorted(body.keys()) if isinstance(body, dict) else [],
                    "fixtureRunMatches": isinstance(body, dict) and body.get("run_id") == FIXTURE_RUN_ID,
                    "fixtureMessageMatches": isinstance(body, dict) and body.get("message_id") == FIXTURE_MESSAGE_ID,
                    "logicalEvents": [
                        {"jsonType": type(entry).__name__,
                         "keys": sorted(entry.keys()) if isinstance(entry, dict) else [],
                         "fixtureRunMatches": isinstance(entry, dict) and entry.get("run_id") == FIXTURE_RUN_ID,
                         "fixtureMessageMatches": isinstance(entry, dict) and entry.get("message_id") == FIXTURE_MESSAGE_ID}
                        for entry in entries[:10]
                    ],
                })
            except json.JSONDecodeError:
                observed.append({"partition": row.partition, "offset": row.offset, "invalidJson": True})
        consumer_observations.append({"batchId": batch_id, "sampleCount": len(samples), "samples": observed})
        process_batch(batch, batch_id, fixture_run_id=FIXTURE_RUN_ID)
    active = None
    try:
        active = (stream.writeStream.foreachBatch(observed_batch)
                  .option("checkpointLocation", CHECKPOINT)
                  .trigger(processingTime="2 seconds").start())
        deadline = time.monotonic() + 180
        ready_written = False
        while time.monotonic() < deadline:
            active.awaitTermination(5)
            if active.exception():
                raise active.exception()
            if not active.isActive:
                raise RuntimeError("FunctionalStreamTerminated")
            if LIVE_FIXTURE and not ready_written and active.lastProgress:
                notebookutils.fs.put(
                    f"Files/diagnostics/functional-ready-{RUN_ID}.json",
                    json.dumps({"runId": RUN_ID, "consumerActive": True,
                                "fixtureRunId": FIXTURE_RUN_ID, "fixtureMessageId": FIXTURE_MESSAGE_ID}), True)
                ready_written = True
            if active.lastProgress and spark.table(TABLE_AUDIT).where(
                    (F.col("run_id") == FIXTURE_RUN_ID)
                    & (F.col("message_id") == FIXTURE_MESSAGE_ID)).limit(1).count() == 1:
                progress = active.lastProgress
                progress = json.loads(progress.json) if hasattr(progress, "json") else dict(progress)
                return {"microbatchCompleted": True, "fixtureTerminalAuditObserved": True,
                        "inputRows": progress["numInputRows"]}
        raise RuntimeError("FunctionalStreamNoProgress")
    finally:
        if active is not None:
            progress = active.lastProgress
            if progress is not None and hasattr(progress, "json"):
                progress = json.loads(progress.json)
            if progress:
                progress = {name: progress.get(name) for name in
                            ("batchId", "numInputRows", "sources", "durationMs")}
            consumer_observations.append({
                "lastProgress": progress,
                "status": active.status,
                "observedFixtureRunId": FIXTURE_RUN_ID,
            })
            active.stop()


def outputs():
    records = {}
    for table in (TABLE_EVENTS, TABLE_SCORES, TABLE_AUDIT, TABLE_QUARANTINE):
        records[table] = [
            row.asDict(recursive=True) for row in spark.table(table)
            .where(F.col("run_id") == FIXTURE_RUN_ID).limit(4).collect()]
    event = records[TABLE_EVENTS]
    score = records[TABLE_SCORES]
    audit = records[TABLE_AUDIT]
    quarantine = records[TABLE_QUARANTINE]
    if (len(event) != 1 or len(score) != 1 or len(audit) != 1 or quarantine
            or event[0]["message_id"] != FIXTURE_MESSAGE_ID
            or score[0]["message_id"] != FIXTURE_MESSAGE_ID
            or event[0]["equipment_id"] != score[0]["equipment_id"]
            or event[0]["message_timestamp"] != score[0]["message_timestamp"]
            or not math.isclose(event[0]["calculated_temperature"], EXPECTED_AVERAGE, abs_tol=0.000001)
            or not 1 <= score[0]["score"] <= 100 or audit[0]["status"] != "SCORED"):
        gate_output_records.update(records)
        raise RuntimeError("FunctionalOutputMismatch")
    gate_output_records.update(records)
    return {"events": 1, "scores": 1, "scoredAudit": 1, "quarantine": 0,
            "expectedAverage": EXPECTED_AVERAGE, "correlationPassed": True}


gate_output_records = {}
gate_stage("advertised_kafka_brokers", broker_metadata)
if LIVE_FIXTURE:
    if gate_stage("warm_kql_transport", warm_kql_transport) and gate_stage("warm_output_writes", warm_output_writes):
        gate_stage("production_fixture_processing", process_fixture)
    gate_stage("spark_parameterized_kql", parameterized_kql)
elif gate_stage("spark_parameterized_kql", parameterized_kql):
    gate_stage("production_fixture_processing", process_fixture)
gate_stage("events_scores_audit_correlation", outputs)
gate_report = {
    "runId": RUN_ID, "fixtureRunId": FIXTURE_RUN_ID, "atUtc": datetime.now(timezone.utc).isoformat(),
    "diagnosticOnly": True, "oapEnabled": False, "acceptancePassed": False,
    "allDiagnosticStagesSucceeded": all(item["status"] == "Succeeded" for item in gate_results),
    "results": gate_results, "outputRecords": gate_output_records,
    "consumerObservations": consumer_observations[-5:],
    "stageTimings": stage_timings,
}
def encode_evidence(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat() if value.tzinfo is None else value.isoformat()
    raise TypeError(f"UnsupportedEvidenceType:{type(value).__name__}")


try:
    gate_json = json.dumps(gate_report, sort_keys=True, default=encode_evidence)
except (TypeError, AttributeError) as error:
    gate_report["allDiagnosticStagesSucceeded"] = False
    gate_report["evidenceCollectionFailed"] = True
    gate_report["serializationErrorType"] = type(error).__name__
    gate_report["serializationErrorCode"] = str(error) if str(error).startswith("UnsupportedEvidenceType:") else "AttributeError"
    gate_report.pop("consumerObservations", None)
    gate_json = json.dumps(gate_report, sort_keys=True, default=encode_evidence)
try:
    notebookutils.fs.mkdirs("Files/diagnostics")
    notebookutils.fs.put(f"Files/diagnostics/functional-processing-{RUN_ID}.json", gate_json, True)
except Exception as error:
    gate_report["evidenceWriteErrorType"] = type(error).__name__
notebookutils.notebook.exit(gate_json)
