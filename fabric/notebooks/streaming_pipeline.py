"""Fabric notebook source for the approved Eventstream-to-Delta processing path.

The deployment script embeds this file as the notebook's code cell and injects
workspace/lakehouse IDs into the notebook metadata, never into secret fields.
"""

import hashlib
import ipaddress
import json
import math
import random
import socket
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from urllib.parse import urlsplit

from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable


spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")

WORKSPACE_ID = "<injected-workspace-id>"
LAKEHOUSE_ID = "<injected-lakehouse-id>"
LAKEHOUSE_NAME = "lhspark"
RUN_ID = "<injected-run-id>"
RUN_MODE = "<injected-run-mode>"
KEY_VAULT_URL = "<injected-private-key-vault-url>"
KUSTO_ENDPOINT = "<injected-kql-query-endpoint>"
KUSTO_DATABASE = "ehspark"
API_URL = "<injected-private-function-url>/api/score"

class KqlQueryError(RuntimeError):
    def __init__(self, code, attempts=0, cause=None):
        super().__init__(code)
        self.attempts = attempts
        self.cause = cause


class ApiRequestError(RuntimeError):
    def __init__(self, code, attempts):
        super().__init__(code)
        self.attempts = attempts

KAFKA_CONNECTION = notebookutils.credentials.getSecret(
    KEY_VAULT_URL, "eventstream-destination-connection"
)
FUNCTION_KEY = notebookutils.credentials.getSecret(
    KEY_VAULT_URL, "score-api-function-key"
)

_connection = dict(
    part.split("=", 1)
    for part in KAFKA_CONNECTION.split(";")
    if "=" in part
)
_endpoint = _connection.get("Endpoint", "")
KAFKA_BOOTSTRAP = f"{_endpoint[5:].rstrip('/')} :9093".replace(" ", "")
KAFKA_TOPIC = _connection.get("EntityPath")
if not _endpoint.startswith("sb://") or not KAFKA_TOPIC:
    raise RuntimeError("Eventstream destination connection is incomplete")

TABLE_EVENTS = f"{LAKEHOUSE_NAME}.Events"
TABLE_SCORES = f"{LAKEHOUSE_NAME}.Scores"
TABLE_AUDIT = f"{LAKEHOUSE_NAME}.ProcessingAudit"
TABLE_QUARANTINE = f"{LAKEHOUSE_NAME}.Quarantine"
CHECKPOINT = (
    f"abfss://{WORKSPACE_ID}@{urlsplit(notebookutils.conf.get('trident.onelake.endpoint')).netloc}/"
    f"{LAKEHOUSE_ID}/Files/checkpoints/{RUN_ID}"
)

spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {TABLE_EVENTS} (
      message_id STRING, run_id STRING, equipment_id INT,
      message_timestamp TIMESTAMP, calculated_temperature DOUBLE, spark_processed_at TIMESTAMP
    ) USING DELTA"""
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {TABLE_SCORES} (
      message_id STRING, run_id STRING, equipment_id INT,
      message_timestamp TIMESTAMP, score INT, spark_processed_at TIMESTAMP
    ) USING DELTA"""
)
for output_table in (TABLE_EVENTS, TABLE_SCORES):
    if "spark_processed_at" not in spark.table(output_table).columns:
        spark.sql(f"ALTER TABLE {output_table} ADD COLUMNS (spark_processed_at TIMESTAMP)")
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {TABLE_AUDIT} (
      message_id STRING, run_id STRING, kafka_topic STRING,
      kafka_partition INT, kafka_offset BIGINT, kafka_timestamp TIMESTAMP,
      payload_json STRING, payload_hash STRING, received_at TIMESTAMP,
      raw_visible_at TIMESTAMP, query_snapshot_utc TIMESTAMP,
      query_unique_count BIGINT, calculated_temperature DOUBLE,
      fallback_reason STRING, events_written_at TIMESTAMP,
      api_attempts INT, api_result STRING, status STRING,
      error_code STRING, updated_at TIMESTAMP
    ) USING DELTA"""
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {TABLE_QUARANTINE} (
      message_id STRING, run_id STRING, payload_json STRING,
      kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
      stage STRING, reason STRING, retry_count INT,
      quarantined_at TIMESTAMP, replay_of STRING, resolution STRING
    ) USING DELTA"""
)

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("message_id", T.StringType()),
        T.StructField("run_id", T.StringType()),
        T.StructField("equipment_id", T.IntegerType()),
        T.StructField("temperature", T.DoubleType()),
        T.StructField("timestamp", T.StringType()),
    ]
)


def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _verify_session_allocation(require_executor=False):
    conf = spark.sparkContext.getConf()
    expected = {
        "spark.dynamicAllocation.enabled": "false",
        "spark.driver.cores": "8",
        "spark.driver.memory": "56g",
        "spark.executor.cores": "8",
        "spark.executor.memory": "56g",
        "spark.executor.instances": "1",
    }
    observed = {key: conf.get(key, "unset") for key in expected}
    if observed != expected:
        raise RuntimeError("FixedExecutorConfigurationNotApplied")
    executors = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1
    if executors not in (0, 1) or (require_executor and executors != 1):
        raise RuntimeError("FixedExecutorCountMismatch")
    return {"checkedAtUtc": datetime.now(timezone.utc).isoformat(),
            "configuration": observed, "activeExecutors": executors,
            "maximumDriverAndExecutorCores": 16}


def _upsert(table_name, rows):
    if not rows:
        return
    frame = spark.createDataFrame(rows, spark.table(table_name).schema).coalesce(1)
    condition = (
        (F.col("t.run_id") == F.col("s.run_id"))
        & (F.col("t.message_id") == F.col("s.message_id"))
        & F.col("t.run_id").isin(list({row["run_id"] for row in rows}))
        & F.col("t.message_id").isin(list({row["message_id"] for row in rows}))
    )
    merge = (
        DeltaTable.forName(spark, table_name)
        .alias("t")
        .merge(frame.alias("s"), condition)
    )
    if table_name in (TABLE_EVENTS, TABLE_SCORES):
        updates = {column: F.col(f"s.{column}") for column in frame.columns}
        updates["spark_processed_at"] = F.coalesce(
            F.col("t.spark_processed_at"), F.col("s.spark_processed_at"))
        merge = merge.whenMatchedUpdate(set=updates)
    else:
        merge = merge.whenMatchedUpdateAll()
    merge.whenNotMatchedInsertAll().execute()


def _audit_for(run_id, message_id):
    rows = (
        spark.table(TABLE_AUDIT)
        .where((F.col("run_id") == run_id) & (F.col("message_id") == message_id))
        .limit(1)
        .collect()
    )
    return rows[0].asDict(recursive=True) if rows else None


def _kusto_token():
    return notebookutils.credentials.getToken("kusto")


def _query_kusto(query, parameters, timeout_seconds=4.0):
    encoded_parameters = {}
    for name, value in parameters.items():
        if isinstance(value, bool):
            value = str(value).lower()
        elif isinstance(value, int):
            value = str(value)
        elif isinstance(value, datetime):
            value = f"datetime({value.isoformat()})"
        encoded_parameters[name] = str(value)
    body = json.dumps(
        {"db": KUSTO_DATABASE, "csl": query,
         "properties": json.dumps({"Options": {}, "Parameters": encoded_parameters})},
        separators=(",", ":"),
    ).encode("utf-8")
    deadline = time.monotonic() + timeout_seconds
    token = _kusto_token()
    last_error = None
    attempts = 0
    for attempt in (1, 2):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        attempts = attempt
        request = urllib.request.Request(
            f"{KUSTO_ENDPOINT.rstrip('/')}/v1/rest/query",
            data=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "x-ms-client-request-id": f"esehspark;{uuid.uuid4()}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=min(2.0, remaining)) as response:
                result = json.loads(response.read().decode("utf-8"))
                if time.monotonic() > deadline:
                    raise KqlQueryError("KQLDeadlineExceeded", attempts, "ResponseCompletedAfterDeadline")
                return result
        except urllib.error.HTTPError as error:
            last_error = f"KQLHttp{error.code}"
            if error.code not in {408, 429, 500, 502, 503, 504} or attempt == 2:
                raise KqlQueryError(last_error, attempts, last_error) from None
            retry_after = error.headers.get("Retry-After")
            try:
                delay = min(float(retry_after), max(0.0, deadline - time.monotonic())) if retry_after else 0.2
            except ValueError:
                delay = 0.2
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            last_error = type(error).__name__
            if attempt == 2:
                raise KqlQueryError("KQLTransportError", attempts, last_error) from None
            delay = 0.2 + random.random() * 0.2
        except (ValueError, json.JSONDecodeError):
            raise KqlQueryError("KQLInvalidResponse") from None
        if delay >= deadline - time.monotonic():
            break
        time.sleep(delay)
    raise KqlQueryError("KQLDeadlineExceeded", attempts, last_error)

def _rows_from_kusto(result, expected_columns):
    if result.get("error"):
        raise KqlQueryError("KQL response contains an error")
    tables = result.get("Tables", result.get("tables", []))
    matching = [
        table for table in tables
        if [column.get("ColumnName", column.get("columnName"))
            for column in table.get("Columns", table.get("columns", []))] == expected_columns
    ]
    if len(matching) != 1:
        raise KqlQueryError("KQL response lacks a unique expected result projection")
    rows = matching[0].get("Rows", matching[0].get("rows"))
    if not isinstance(rows, list) or any(
        not isinstance(row, list) or len(row) != len(expected_columns) for row in rows
    ):
        raise KqlQueryError("KQL response has invalid result rows")
    return rows


def _eventhouse_average(event):
    aggregate_query = """
      declare query_parameters(runId:string, messageId:string,
                               equipmentId:int, triggerTime:datetime);
      RawEvents
      | where equipment_id == equipmentId
      | where timestamp between (triggerTime - 20m .. triggerTime)
      | summarize temperature=any(temperature), timestamp=any(timestamp) by run_id, message_id
      | summarize average=avg(temperature), unique_count=count(),
                  trigger_visible=countif(run_id == runId and message_id == messageId),
                  window_records=make_list(bag_pack("run_id", run_id, "message_id", message_id,
                                                   "temperature", temperature, "timestamp", timestamp))
    """
    trigger_time = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    aggregate = _rows_from_kusto(
        _query_kusto(
            aggregate_query,
            {
                "runId": event["run_id"],
                "messageId": event["message_id"],
                "equipmentId": event["equipment_id"],
                "triggerTime": trigger_time,
            },
        ),
        ["average", "unique_count", "trigger_visible", "window_records"],
    )
    if len(aggregate) != 1:
        raise KqlQueryError("KQLInvalidAggregate")
    average, unique_count, visible, window = aggregate[0]
    if isinstance(window, str):
        try:
            window = json.loads(window)
        except ValueError:
            raise KqlQueryError("KQLInvalidAggregate") from None
    # Kusto avg() returns NaN for an empty input, including a REST "NaN" sentinel.
    if (type(unique_count) is int and unique_count == 0 and visible == 0 and window == []
            and (average == "NaN" or (type(average) is float and math.isnan(average)))):
        average = None
    if (type(unique_count) is not int or unique_count < 0
            or type(visible) is not int or visible not in (0, 1) or visible > unique_count
            or not isinstance(window, list) or len(window) != unique_count
            or (average is not None and (
                isinstance(average, bool) or not isinstance(average, (int, float))
                or not math.isfinite(average) or not 0 <= average <= 100))):
        raise KqlQueryError("KQLInvalidAggregate")
    snapshot = _utcnow()
    return (
        None if average is None else float(average),
        unique_count,
        snapshot if visible else None,
        snapshot,
    )


def _call_score_api(event, calculated_temperature):
    body = json.dumps(
        {
            "message_id": event["message_id"],
            "run_id": event["run_id"],
            "equipment_id": event["equipment_id"],
            "timestamp": event["timestamp"],
            "calculated_temperature": calculated_temperature,
        },
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    idempotency_key = f"{event['run_id']}:{event['message_id']}"
    deadline = time.monotonic() + 8.0
    retryable_statuses = {408, 429, 500, 502, 503, 504}
    last_error = "TransportError"

    for attempt in (1, 2):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        request = urllib.request.Request(
            API_URL,
            data=body,
            headers={
                "Content-Type": "application/json",
                "x-functions-key": FUNCTION_KEY,
                "Idempotency-Key": idempotency_key,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=min(3.0, remaining)) as response:
                result = json.loads(response.read().decode("utf-8"))
            if time.monotonic() > deadline:
                raise ApiRequestError("DeadlineExceeded", attempt)
            if (
                result.get("message_id") != event["message_id"]
                or result.get("run_id") != event["run_id"]
                or isinstance(result.get("score"), bool)
                or not isinstance(result.get("score"), int)
                or not 1 <= result["score"] <= 100
            ):
                raise ValueError("InvalidScoringResponse")
            return result["score"], attempt
        except urllib.error.HTTPError as error:
            last_error = f"HTTP{error.code}"
            if error.code not in retryable_statuses or attempt == 2:
                raise ApiRequestError(last_error, attempt) from None
            retry_after = error.headers.get("Retry-After")
            try:
                delay = min(float(retry_after), max(0.0, deadline - time.monotonic())) if retry_after else 0.2
            except ValueError:
                delay = 0.2
        except (urllib.error.URLError, TimeoutError, OSError):
            last_error = "TransportError"
            if attempt == 2:
                raise ApiRequestError(last_error, attempt) from None
            delay = 0.2 + random.random() * 0.2
        except (ValueError, AttributeError, TypeError):
            raise ApiRequestError("InvalidScoringResponse", attempt) from None
        if delay >= deadline - time.monotonic():
            break
        time.sleep(delay)
    raise ApiRequestError(last_error, min(attempt, 2))


def _run_private_route_canary():
    function_host = urlsplit(API_URL).hostname
    vault_host = urlsplit(KEY_VAULT_URL).hostname
    if not function_host or not vault_host:
        raise RuntimeError("PrivateRouteCanaryHostMissing")

    function_ips = sorted(
        {
            entry[4][0]
            for entry in socket.getaddrinfo(function_host, 443, type=socket.SOCK_STREAM)
        }
    )
    vault_ips = sorted(
        {
            entry[4][0]
            for entry in socket.getaddrinfo(vault_host, 443, type=socket.SOCK_STREAM)
        }
    )
    if not function_ips or not vault_ips:
        raise RuntimeError("PrivateRouteCanaryDnsEmpty")
    if not all(ipaddress.ip_address(address).is_private for address in function_ips + vault_ips):
        raise RuntimeError("PrivateRouteCanaryResolvedPublicAddress")

    event = {
        "message_id": str(uuid.uuid4()),
        "run_id": RUN_ID,
        "equipment_id": 17,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        ),
    }
    calculated_temperature = 82.5
    score, attempts = _call_score_api(event, calculated_temperature)
    if isinstance(score, bool) or not isinstance(score, int) or not 1 <= score <= 100:
        raise RuntimeError("PrivateRouteCanaryScoreMismatch")

    report = {
        "run_id": RUN_ID,
        "message_id": event["message_id"],
        "function_host": function_host,
        "function_private_addresses": function_ips,
        "key_vault_host": vault_host,
        "key_vault_private_addresses": vault_ips,
        "key_vault_secret_read": bool(FUNCTION_KEY),
        "tls_scoring_status": "Succeeded",
        "api_attempts": attempts,
        "score": score,
        "completed_at_utc": _utcnow().isoformat() + "Z",
    }
    _upsert(
        TABLE_AUDIT,
        [
            {
                "message_id": event["message_id"],
                "run_id": RUN_ID,
                "kafka_topic": KAFKA_TOPIC,
                "kafka_partition": None,
                "kafka_offset": None,
                "kafka_timestamp": None,
                "payload_json": json.dumps(report, sort_keys=True, separators=(",", ":")),
                "payload_hash": hashlib.sha256(
                    json.dumps(report, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest(),
                "received_at": _utcnow(),
                "raw_visible_at": None,
                "query_snapshot_utc": None,
                "query_unique_count": None,
                "calculated_temperature": calculated_temperature,
                "fallback_reason": None,
                "events_written_at": None,
                "api_attempts": attempts,
                "api_result": str(score),
                "status": "PRIVATE_CANARY",
                "error_code": None,
                "updated_at": _utcnow(),
            }
        ],
    )
    saved = _audit_for(RUN_ID, event["message_id"])
    if not saved or saved.get("status") != "PRIVATE_CANARY":
        raise RuntimeError("PrivateRouteCanaryAuditReadbackFailed")
    print("SPARK_PRIVATE_ROUTE_CANARY:" + json.dumps(report, sort_keys=True))


def _quarantine(event, kafka, stage, reason, retry_count=0, raw_payload=None, write=None):
    payload_json = raw_payload
    if payload_json is None:
        payload_json = (
            event
            if isinstance(event, str)
            else json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False)
        )
    (write or _upsert)(
        TABLE_QUARANTINE,
        [
            {
                "message_id": event.get("message_id") if isinstance(event, dict) else None,
                "run_id": event.get("run_id") if isinstance(event, dict) else None,
                "payload_json": payload_json,
                "kafka_topic": kafka["topic"],
                "kafka_partition": kafka["partition"],
                "kafka_offset": kafka["offset"],
                "stage": stage,
                "reason": reason,
                "retry_count": retry_count,
                "quarantined_at": _utcnow(),
                "replay_of": None,
                "resolution": None,
            }
        ],
    )


def _payload_digest(payload_json):
    payload = json.loads(payload_json)
    if not isinstance(payload, dict):
        raise ValueError("IdentityPayloadNotObject")
    # Eventstream adds these transport fields anew on each logical resend.
    logical = {key: value for key, value in payload.items()
               if key not in {"EventEnqueuedUtcTime", "EventProcessedUtcTime", "PartitionId"}}
    encoded = json.dumps(logical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _audit_payload_matches(audit, payload_json):
    retained = audit["payload_json"]
    retained_digest = _payload_digest(retained)
    legacy_digest = hashlib.sha256(retained.encode("utf-8")).hexdigest()
    if audit["payload_hash"] not in (retained_digest, legacy_digest):
        raise RuntimeError("RetainedAuditPayloadHashMismatch")
    return retained_digest == _payload_digest(payload_json)


def _write_failure_audit(event, kafka, payload_json, stage, reason, write=None):
    run_id = event.get("run_id") if isinstance(event, dict) else None
    message_id = event.get("message_id") if isinstance(event, dict) else None
    payload_hash = (_payload_digest(payload_json) if stage == "KQL"
                    else hashlib.sha256(payload_json.encode("utf-8")).hexdigest())
    (write or _upsert)(
        TABLE_AUDIT,
        [
            {
                "message_id": message_id,
                "run_id": run_id,
                "kafka_topic": kafka["topic"],
                "kafka_partition": kafka["partition"],
                "kafka_offset": kafka["offset"],
                "kafka_timestamp": kafka["timestamp"],
                "payload_json": payload_json,
                "payload_hash": payload_hash,
                "received_at": _utcnow(),
                "raw_visible_at": None,
                "query_snapshot_utc": None,
                "query_unique_count": None,
                "calculated_temperature": None,
                "fallback_reason": None,
                "events_written_at": None,
                "api_attempts": 0,
                "api_result": None,
                "status": "QUARANTINED",
                "error_code": f"{stage}:{reason}",
                "updated_at": _utcnow(),
            }
        ],
    )


def _process_record(row, context=None):
    write = context.upsert if context else _upsert
    read_audit = context.audit_for if context else _audit_for
    payload_json = row["value"]
    kafka = {
        "topic": row["topic"],
        "partition": row["partition"],
        "offset": row["offset"],
        "timestamp": row["timestamp"],
        "envelope_index": row.get("envelope_index", 0),
    }
    event = row.get("parsed")
    try:
        if event is None:
            raise ValueError("InvalidJson")
        event = event.asDict(recursive=True) if hasattr(event, "asDict") else event
        if not isinstance(event, dict):
            raise ValueError("JSONNotObject")
        message_id = str(uuid.UUID(event["message_id"]))
        run_id = str(uuid.UUID(event["run_id"]))
        if message_id != event["message_id"].lower():
            raise ValueError("InvalidMessageId")
        if run_id != event["run_id"].lower():
            raise ValueError("InvalidRunId")
        event["message_id"] = message_id
        event["run_id"] = run_id
        if (
            isinstance(event["equipment_id"], bool)
            or not isinstance(event["equipment_id"], int)
            or not 0 <= event["equipment_id"] <= 100
        ):
            raise ValueError("InvalidEquipmentId")
        if isinstance(event["temperature"], bool) or not isinstance(
            event["temperature"], (int, float)
        ):
            raise ValueError("InvalidTemperature")
        temperature = float(event["temperature"])
        if not math.isfinite(temperature) or not 0 <= temperature <= 100:
            raise ValueError("InvalidTemperature")
        if temperature <= 80:
            raise ValueError("FilterContractViolation")
        timestamp = event["timestamp"]
        if not isinstance(timestamp, str) or not (
            timestamp.endswith("Z") or timestamp.endswith("+00:00")
        ):
            raise ValueError("InvalidUtcTimestamp")
        parsed_timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if parsed_timestamp.utcoffset() != timezone.utc.utcoffset(parsed_timestamp):
            raise ValueError("InvalidUtcTimestamp")
        event["timestamp"] = (
            parsed_timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        )
        event["temperature"] = temperature
    except Exception as error:
        failed_event = dict(event) if isinstance(event, dict) else {}
        transport_key = (
            f"{kafka['topic']}:{kafka['partition']}:{kafka['offset']}:"
            f"{kafka['envelope_index']}"
        )
        failed_event["run_id"] = (failed_event.get("run_id") if isinstance(failed_event.get("run_id"), str)
                                  and failed_event["run_id"] else None) or str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"kafka-invalid:{kafka['topic']}")
        )
        failed_event["message_id"] = (failed_event.get("message_id") if isinstance(failed_event.get("message_id"), str)
                                      and failed_event["message_id"] else None) or str(
            uuid.uuid5(uuid.NAMESPACE_URL, transport_key)
        )
        reason = f"{type(error).__name__}:envelope_index={kafka['envelope_index']}"
        _write_failure_audit(failed_event, kafka, payload_json, "VALIDATION", reason, write=write)
        _quarantine(
            failed_event,
            kafka,
            "VALIDATION",
            reason,
            raw_payload=payload_json,
            write=write,
        )
        return

    payload_hash = _payload_digest(payload_json)
    audit = read_audit(event["run_id"], event["message_id"])
    if audit and not _audit_payload_matches(audit, payload_json):
        _quarantine(event, kafka, "IDENTITY", "RunMessagePayloadConflict",
                    raw_payload=payload_json, write=write)
        return
    if audit and audit.get("status") == "SCORED":
        return

    try:
        if audit and audit.get("calculated_temperature") is not None:
            calculated = float(audit["calculated_temperature"])
            visible_at = audit.get("raw_visible_at")
            snapshot = audit.get("query_snapshot_utc")
            unique_count = audit.get("query_unique_count")
            fallback_reason = audit.get("fallback_reason")
        else:
            average, unique_count, visible_at, snapshot = _eventhouse_average(event)
            fallback_reason = None
            if average is None:
                calculated = temperature
                fallback_reason = "SuccessfulEmptyOrNullKqlResult"
            else:
                calculated = average

        received_at = _utcnow()
        write(
            TABLE_EVENTS,
            [
                {
                    "message_id": event["message_id"],
                    "run_id": event["run_id"],
                    "equipment_id": event["equipment_id"],
                    "message_timestamp": datetime.fromisoformat(
                        event["timestamp"].replace("Z", "+00:00")
                    ).astimezone(timezone.utc).replace(tzinfo=None),
                    "calculated_temperature": calculated,
                    "spark_processed_at": _utcnow(),
                }
            ],
        )
        events_written_at = _utcnow()
        prepared_audit = {
            "message_id": event["message_id"],
            "run_id": event["run_id"],
            "kafka_topic": kafka["topic"],
            "kafka_partition": kafka["partition"],
            "kafka_offset": kafka["offset"],
            "kafka_timestamp": kafka["timestamp"],
            "payload_json": payload_json,
            "payload_hash": payload_hash,
            "received_at": received_at,
            "raw_visible_at": visible_at,
            "query_snapshot_utc": snapshot,
            "query_unique_count": unique_count,
            "calculated_temperature": calculated,
            "fallback_reason": fallback_reason,
            "events_written_at": events_written_at,
            "api_attempts": 0,
            "api_result": None,
            "status": "EVENTS_WRITTEN",
            "error_code": None,
            "updated_at": events_written_at,
        }
        write(TABLE_AUDIT, [prepared_audit])
    except Exception as error:
        # KQL/query errors may be quarantined; Delta persistence errors must fail
        # the batch so Spark does not advance its checkpoint.
        if isinstance(error, (KqlQueryError, TimeoutError, urllib.error.URLError, urllib.error.HTTPError)):
            reason = str(error) if isinstance(error, KqlQueryError) else type(error).__name__
            _write_failure_audit(event, kafka, payload_json, "KQL", reason, write=write)
            _quarantine(event, kafka, "KQL", reason, write=write)
            return
        raise

    if context is not None:
        context.scoring.append((event, calculated, kafka, prepared_audit))
        return
    _score_record(event, calculated, kafka, prepared_audit, write)


def _score_record(event, calculated, kafka, prepared_audit, write):
    try:
        score, attempts = _call_score_api(event, calculated)
    except Exception as error:
        attempts = getattr(error, "attempts", 0)
        reason = str(error) if isinstance(error, ApiRequestError) else type(error).__name__
        write(
            TABLE_AUDIT,
            [
                {
                    **prepared_audit,
                    "api_attempts": attempts,
                    "api_result": None,
                    "status": "API_FAILED",
                    "error_code": reason,
                    "updated_at": _utcnow(),
                }
            ],
        )
        _quarantine(event, kafka, "API", reason, retry_count=attempts, write=write)
        return

    write(
        TABLE_SCORES,
        [
            {
                "message_id": event["message_id"],
                "run_id": event["run_id"],
                "equipment_id": event["equipment_id"],
                "message_timestamp": datetime.fromisoformat(
                    event["timestamp"].replace("Z", "+00:00")
                ).astimezone(timezone.utc).replace(tzinfo=None),
                "score": score,
                "spark_processed_at": _utcnow(),
            }
        ],
    )
    write(
        TABLE_AUDIT,
        [
            {
                **prepared_audit,
                "api_attempts": attempts,
                "api_result": str(score),
                "status": "SCORED",
                "error_code": None,
                "updated_at": _utcnow(),
            }
        ],
    )


class BatchWrites:
    def __init__(self, audit):
        self.audit = audit
        self.pending = {}
        self.scoring = []

    def audit_for(self, run_id, message_id):
        return self.audit.get((run_id, message_id))

    def upsert(self, table, rows):
        pending = self.pending.setdefault(table, {})
        for row in rows:
            key = (row["run_id"], row["message_id"])
            pending[key] = row
            if table == TABLE_AUDIT:
                self.audit[key] = row

    def flush(self, table):
        rows = list(self.pending.get(table, {}).values())
        if rows:
            _upsert(table, rows)
            self.pending[table] = {}


def process_records(rows):
    if not rows:
        return
    run_ids = {row["parsed"].get("run_id") for row in rows
               if isinstance(row.get("parsed"), dict)
               and isinstance(row["parsed"].get("run_id"), str)}
    message_ids = {row["parsed"].get("message_id") for row in rows
                   if isinstance(row.get("parsed"), dict)
                   and isinstance(row["parsed"].get("message_id"), str)}
    audit = {}
    if run_ids and message_ids:
        for row in spark.table(TABLE_AUDIT).where(
                F.col("run_id").isin(sorted(run_ids))
                & F.col("message_id").isin(sorted(message_ids))).collect():
            saved = row.asDict(recursive=True)
            audit[(saved["run_id"], saved["message_id"])] = saved
    context = BatchWrites(audit)
    seen = {}
    for row in rows:
        event = row.get("parsed")
        key = (event.get("run_id"), event.get("message_id")) if isinstance(event, dict) else None
        if key and all(isinstance(value, str) and value for value in key):
            if key in seen:
                if seen[key] != _payload_digest(row["value"]):
                    _process_record(row, context)
                continue
            seen[key] = _payload_digest(row["value"])
        _process_record(row, context)
    # Events and their enrichment snapshot must be durable before any API call.
    context.flush(TABLE_EVENTS)
    for _, _, _, prepared_audit in context.scoring:
        prepared_audit["events_written_at"] = _utcnow()
        prepared_audit["updated_at"] = prepared_audit["events_written_at"]
    for table in (TABLE_AUDIT, TABLE_QUARANTINE):
        context.flush(table)
    for event, calculated, kafka, prepared_audit in context.scoring:
        _score_record(event, calculated, kafka, prepared_audit, context.upsert)
    for table in (TABLE_SCORES, TABLE_AUDIT, TABLE_QUARANTINE):
        context.flush(table)


def replay_quarantined(run_id, message_id):
    run_id, message_id = str(uuid.UUID(run_id)), str(uuid.UUID(message_id))
    rows = spark.table(TABLE_QUARANTINE).where(
        (F.col("run_id") == run_id) & (F.col("message_id") == message_id)).limit(2).collect()
    if len(rows) != 1:
        raise RuntimeError("QuarantineReplayRequiresOneRecord")
    quarantined = rows[0].asDict(recursive=True)
    if quarantined["stage"] not in ("KQL", "API"):
        raise RuntimeError("QuarantineReplayRequiresCorrectedValidationOrIdentityPayload")
    event = json.loads(quarantined["payload_json"])
    if not isinstance(event, dict) or event.get("run_id") != run_id or event.get("message_id") != message_id:
        raise RuntimeError("QuarantineReplayIdentityMismatch")
    audit = _audit_for(run_id, message_id)
    if audit is None:
        raise RuntimeError("QuarantineReplayAuditMissing")
    if audit["status"] != "SCORED":
        process_records([{
            "parsed": event, "value": quarantined["payload_json"],
            "topic": quarantined["kafka_topic"], "partition": quarantined["kafka_partition"],
            "offset": quarantined["kafka_offset"], "timestamp": audit.get("kafka_timestamp"),
            "envelope_index": 0,
        }])
        audit = _audit_for(run_id, message_id)
    if audit is None or audit["status"] != "SCORED":
        raise RuntimeError("QuarantineReplayDidNotScore")
    if not _audit_payload_matches(audit, quarantined["payload_json"]):
        raise RuntimeError("QuarantineReplayPayloadConflict")
    resolution = json.dumps({
        "status": "RESOLVED", "execution_run_id": RUN_ID, "resolved_at": _utcnow().isoformat(),
    }, separators=(",", ":"))
    _upsert(TABLE_QUARANTINE, [{
        **quarantined, "replay_of": f"{run_id}:{message_id}", "resolution": resolution,
    }])
    return {"run_id": run_id, "message_id": message_id, "status": "SCORED"}


def decode_kafka_envelope(row):
    payload_json = row["value"]
    try:
        body = json.loads(payload_json)
    except (json.JSONDecodeError, TypeError):
        return [dict(row, parsed=None, envelope_index=0)]
    entries = body if isinstance(body, list) and body else [body]
    return [
        dict(row, value=json.dumps(entry, sort_keys=True, separators=(",", ":")),
             parsed=entry, envelope_index=index)
        for index, entry in enumerate(entries)
    ]


def process_batch(batch_df, batch_id, fixture_run_id=None):
    if batch_df.isEmpty():
        return
    parsed = batch_df.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp"),
        F.col("value").cast("string").alias("value"),
    )
    records = []
    for row in parsed.toLocalIterator():
        for event_row in decode_kafka_envelope(row.asDict(recursive=True)):
            event = event_row.get("parsed")
            if fixture_run_id is not None and (
                isinstance(event, dict) and event.get("run_id")
                and event.get("run_id") != fixture_run_id
            ):
                continue
            records.append(event_row)
    process_records(records)


if RUN_MODE == "private-canary":
    _run_private_route_canary()
    notebookutils.notebook.exit("PRIVATE_ROUTE_CANARY_OK")
if RUN_MODE != "streaming":
    raise RuntimeError(f"Unsupported notebook run mode: {RUN_MODE}")

_verify_session_allocation()
stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("kafka.group.id", f"esehspark-{RUN_ID}")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "true")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        'username="$ConnectionString" '
        f'password="{KAFKA_CONNECTION}";',
    )
    .load()
    .select("topic", "partition", "offset", "timestamp", "value")
)

query = (
    stream.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="1 second")
    .start()
)
ready_deadline = time.monotonic() + 180.0
while query.isActive and query.lastProgress is None and time.monotonic() < ready_deadline:
    time.sleep(1.0)
if not query.isActive:
    raise RuntimeError("Kafka streaming query terminated before reaching READY")
if query.lastProgress is None:
    query.stop()
    raise TimeoutError("Kafka streaming query did not produce an initial progress record")
_upsert(
    TABLE_AUDIT,
    [
        {
            "message_id": None,
            "run_id": RUN_ID,
            "kafka_topic": KAFKA_TOPIC,
            "kafka_partition": None,
            "kafka_offset": None,
            "kafka_timestamp": None,
            "payload_json": None,
            "payload_hash": None,
            "received_at": _utcnow(),
            "raw_visible_at": None,
            "query_snapshot_utc": None,
            "query_unique_count": None,
            "calculated_temperature": None,
            "fallback_reason": None,
            "events_written_at": None,
            "api_attempts": 0,
            "api_result": None,
            "status": "READY",
            "error_code": None,
            "updated_at": _utcnow(),
        }
    ],
)
query.awaitTermination()
