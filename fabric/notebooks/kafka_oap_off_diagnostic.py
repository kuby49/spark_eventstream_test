"""User-authorized Kafka connectivity test with OAP disabled; no baseline or API calls."""

import json
import re
import socket
from datetime import datetime, timezone
from urllib.parse import urlsplit

RUN_ID = "<injected-run-id>"
HOST = "esehusw3qzfnf3vqakins22j.servicebus.windows.net"
TOPIC = "des_c4fa9b23-3070-4663-9473-dee28aace61c"
results = []
state = {}


def stage(name, operation):
    try:
        detail = operation()
        results.append({"stage": name, "status": "Succeeded", "detail": detail})
        return True
    except Exception as error:
        results.append({
            "stage": name, "status": "Failed", "errorType": type(error).__name__,
            "errorCodes": sorted(set(re.findall(
                r"\b(?:UnknownHostException|TimeoutException|SaslAuthenticationException|"
                r"TopicAuthorizationException|SSLHandshakeException|DATA_SOURCE_NOT_FOUND|"
                r"ForbiddenByRbac|ForbiddenByFirewall|EndpointIdentityMismatch)\b", str(error)))),
        })
        return False


def readiness():
    spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")
    return {"sparkVersion": spark.version, "count": spark.range(1).count()}


def endpoint_dns():
    addresses = sorted({entry[4][0] for entry in socket.getaddrinfo(
        HOST, 9093, type=socket.SOCK_STREAM)})
    state["addresses"] = addresses
    return {"host": HOST, "addresses": addresses, "publicRouteExplicitlyAuthorized": True}


def credentials():
    value = notebookutils.credentials.getSecret(
        "https://esehsparkkv.vault.azure.net/", "eventstream-destination-connection")
    fields = dict(part.split("=", 1) for part in value.split(";") if "=" in part)
    if urlsplit(fields["Endpoint"]).hostname != HOST or fields.get("EntityPath") != TOPIC:
        raise RuntimeError("EndpointIdentityMismatch")
    state["connection"] = value
    return {"host": HOST, "topic": TOPIC}


def consume():
    connection = state["connection"].replace("\\", "\\\\").replace('"', '\\"')
    options = {
        "kafka.bootstrap.servers": f"{HOST}:9093", "subscribe": TOPIC,
        "kafka.security.protocol": "SASL_SSL", "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config":
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="$ConnectionString" password="{connection}";',
        "kafka.request.timeout.ms": "15000", "kafka.default.api.timeout.ms": "30000",
        "startingOffsets": "earliest", "maxOffsetsPerTrigger": "10",
        "kafka.group.id": "esehspark-kafka-test-" + RUN_ID,
    }
    query = None
    state["kafkaConnectionAttempted"] = True
    try:
        stream = spark.readStream.format("kafka").options(**options).load()
        query = (stream.selectExpr("topic", "partition", "offset", "CAST(value AS STRING) AS value")
                 .writeStream.format("memory").queryName("kafka_" + RUN_ID.replace("-", ""))
                 .option("checkpointLocation", f"Files/diagnostics/kafka-oap-off-{RUN_ID}-checkpoint")
                 .trigger(processingTime="5 seconds").start())
        query.awaitTermination(75)
        if query.exception():
            raise query.exception()
        progress = query.recentProgress
        if not progress:
            raise RuntimeError("NoCompletedKafkaMicrobatch")
        return {"completedMicrobatches": len(progress),
                "inputRows": sum(item["numInputRows"] for item in progress),
                "authenticatedKafkaProgressObserved": True}
    finally:
        if query is not None:
            query.stop()


ready = stage("spark_readiness", readiness)
dns = stage("kafka_dns", endpoint_dns)
if ready and dns and stage("destination_secret_identity", credentials):
    stage("kafka_authenticated_stream", consume)
report = {
    "runId": RUN_ID, "atUtc": datetime.now(timezone.utc).isoformat(),
    "diagnosticOnly": True, "acceptancePassed": False, "oapDisabledByUser": True,
    "host": HOST, "topic": TOPIC, "resolvedAddresses": state.get("addresses", []),
    "kafkaConnectionAttempted": state.get("kafkaConnectionAttempted", False),
    "allDiagnosticStagesSucceeded": all(item["status"] == "Succeeded" for item in results),
    "results": results,
}
state.clear()
try:
    notebookutils.fs.mkdirs("Files/diagnostics")
    notebookutils.fs.put(f"Files/diagnostics/kafka-oap-off-{RUN_ID}.json",
                         json.dumps(report, sort_keys=True), True)
except Exception as error:
    report["evidenceWriteErrorType"] = type(error).__name__
notebookutils.notebook.exit(json.dumps(report, sort_keys=True))
