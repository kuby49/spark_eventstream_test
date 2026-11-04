"""Bounded native Event Hubs source diagnostic; never connect over a public route."""

import ipaddress
import json
import re
import socket
from datetime import datetime, timezone
from urllib.parse import urlsplit

RUN_ID = "<injected-run-id>"
EXPECTED_HOST = "esehusw3qzfnf3vqakins22j.servicebus.windows.net"
EXPECTED_ENTITY = "des_c4fa9b23-3070-4663-9473-dee28aace61c"
RESULT_PATH = f"Files/diagnostics/eventhubs-amqp-{RUN_ID}.json"
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
                r"\b(?:ForbiddenByRbac|ForbiddenByFirewall|Unauthorized|"
                r"ClassNotFoundException|DATA_SOURCE_NOT_FOUND|UnknownHostException|"
                r"NoSuchMethodError|PrivateRouteRequired|EndpointIdentityMismatch)\b", str(error)
            ))),
        })
        return False


def readiness():
    spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")
    return {"sparkVersion": spark.version, "count": spark.range(1).count()}


def connector():
    loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    loader.loadClass("org.apache.spark.sql.eventhubs.EventHubsSourceProvider")
    loader.loadClass("org.apache.spark.eventhubs.EventHubsUtils")
    return {"format": "eventhubs", "providerClassAvailable": True}


def private_dns():
    addresses = sorted({entry[4][0] for entry in socket.getaddrinfo(
        EXPECTED_HOST, 5671, type=socket.SOCK_STREAM)})
    state["addresses"] = addresses
    if not addresses or not all(
        ipaddress.ip_address(address) in ipaddress.ip_network("10.0.0.0/8")
        for address in addresses
    ):
        raise RuntimeError("PrivateRouteRequired")
    return {"host": EXPECTED_HOST, "addresses": addresses, "port": 5671}


def credentials():
    value = notebookutils.credentials.getSecret(
        "https://esehsparkkv.vault.azure.net/", "eventstream-destination-connection")
    fields = dict(part.split("=", 1) for part in value.split(";") if "=" in part)
    if (urlsplit(fields["Endpoint"]).hostname != EXPECTED_HOST
            or fields.get("EntityPath") != EXPECTED_ENTITY):
        raise RuntimeError("EndpointIdentityMismatch")
    state["connection"] = value
    return {"host": EXPECTED_HOST, "entity": EXPECTED_ENTITY, "nonempty": bool(value)}


def native_source():
    private_dns()
    options = {
        "eventhubs.connectionString":
            spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(state["connection"]),
        "eventhubs.consumerGroup": "$Default",
        "eventhubs.operationTimeout": "PT15S",
        "eventhubs.receiverTimeout": "PT15S",
        "eventhubs.useExclusiveReceiver": "false",
        "maxEventsPerTrigger": "10",
        "eventhubs.startingPosition": json.dumps({
            "offset": "-1", "seqNo": -1, "enqueuedTime": None, "isInclusive": True}),
    }
    query = None
    state["amqpConnectionAttempted"] = True
    try:
        stream = spark.readStream.format("eventhubs").options(**options).load()
        query = (stream.selectExpr("CAST(body AS STRING) AS body")
                 .writeStream.format("memory").queryName("amqp_" + RUN_ID.replace("-", ""))
                 .option("checkpointLocation", f"Files/diagnostics/eventhubs-amqp-{RUN_ID}-checkpoint")
                 .trigger(processingTime="5 seconds").start())
        query.awaitTermination(45)
        if query.exception():
            raise query.exception()
        progress = query.recentProgress
        if not progress:
            raise RuntimeError("NoCompletedEventHubsMicrobatch")
        return {
            "completedMicrobatches": len(progress),
            "inputRows": sum(item["numInputRows"] for item in progress),
            "protocol": "EventHubs-AMQP", "endToEndAcceptanceProven": False,
        }
    finally:
        if query is not None:
            query.stop()


stage("spark_readiness", readiness)
has_connector = stage("native_eventhubs_connector", connector)
has_private_route = stage("eventhubs_private_dns", private_dns)
if has_connector and has_private_route:
    if stage("destination_secret_identity", credentials):
        stage("native_eventhubs_stream", native_source)
report = {
    "runId": RUN_ID, "atUtc": datetime.now(timezone.utc).isoformat(),
    "diagnosticOnly": True, "acceptancePassed": False,
    "host": EXPECTED_HOST, "entity": EXPECTED_ENTITY,
    "resolvedAddresses": state.get("addresses", []),
    "amqpConnectionAttempted": state.get("amqpConnectionAttempted", False),
    "allDiagnosticStagesSucceeded": all(item["status"] == "Succeeded" for item in results),
    "results": results,
}
state.clear()
try:
    notebookutils.fs.mkdirs("Files/diagnostics")
    notebookutils.fs.put(RESULT_PATH, json.dumps(report, sort_keys=True), True)
except Exception as error:
    report["evidenceWriteErrorType"] = type(error).__name__
notebookutils.notebook.exit(json.dumps(report, sort_keys=True))
