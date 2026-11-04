"""Bounded Fabric prerequisite diagnostics; no Kafka consumer or producer."""

import ipaddress
import json
import re
import socket
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlsplit

RUN_ID = "<injected-run-id>"
WORKSPACE_ID = "<injected-workspace-id>"
LAKEHOUSE_ID = "<injected-lakehouse-id>"
RESULT_PATH = f"Files/diagnostics/{RUN_ID}.json"
PROBE_EXTERNAL_BLOCK = "<injected-outbound-deny-probe>" == "true"
results = []
secrets = {}


def save():
    notebookutils.fs.mkdirs("Files/diagnostics")
    notebookutils.fs.put(RESULT_PATH, json.dumps(results, sort_keys=True), True)


def stage(name, operation):
    try:
        detail = operation()
        result = {"stage": name, "status": "Succeeded", "detail": detail}
    except Exception as error:
        message = str(error)
        codes = sorted(set(re.findall(
            r"\b(?:ForbiddenByFirewall|ForbiddenByConnection|ForbiddenByRbac|Forbidden|"
            r"Unauthorized|AccessDenied|SecretNotFound|BadParameter|"
            r"UnknownHostException|ConnectTimeoutException|SSLHandshakeException|"
            r"NoSuchMethodError|IllegalArgumentException|NameResolutionFailure)\b",
            message,
        )))
        result = {
            "stage": name,
            "status": "Failed",
            "errorType": type(error).__name__,
            "errorCodes": codes,
            "traceback": [
                {"file": frame.filename, "line": frame.lineno, "function": frame.name}
                for frame in traceback.extract_tb(error.__traceback__)
            ],
        }
    result["atUtc"] = datetime.now(timezone.utc).isoformat()
    results.append(result)
    print("PREREQUISITE_STAGE:" + json.dumps(result, sort_keys=True))
    try:
        save()
    except Exception as error:
        print("PREREQUISITE_EVIDENCE_WRITE_FAILED:" + type(error).__name__)


stage("spark_readiness", lambda: {"count": spark.range(1).count(), "version": spark.version})


def onelake_readback():
    if json.loads(notebookutils.fs.head(RESULT_PATH)) != results:
        raise RuntimeError("DiagnosticOneLakeReadbackMismatch")
    return {"matches": True}


stage("onelake_readback", onelake_readback)


def secret(name):
    value = notebookutils.credentials.getSecret("https://esehsparkkv.vault.azure.net", name)
    if not value:
        raise RuntimeError("EmptySecret")
    secrets[name] = value
    return {"nonempty": True}


stage("destination_secret", lambda: secret("eventstream-destination-connection"))
stage("function_secret", lambda: secret("score-api-function-key"))
stage("onelake_endpoint_lookup", lambda: {"endpoint": notebookutils.conf.get("trident.onelake.endpoint")})


def checkpoint_readback():
    host = urlsplit(notebookutils.conf.get("trident.onelake.endpoint")).netloc
    path = f"abfss://{WORKSPACE_ID}@{host}/{LAKEHOUSE_ID}/Files/diagnostics/{RUN_ID}-checkpoint.json"
    notebookutils.fs.put(path, json.dumps({"runId": RUN_ID}), True)
    if json.loads(notebookutils.fs.head(path)) != {"runId": RUN_ID}:
        raise RuntimeError("CheckpointReadbackMismatch")
    return {"matches": True, "host": host}


stage("checkpoint_abfss_readback", checkpoint_readback)


def delta():
    table = "lhspark.PrerequisiteDiagnostic"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} (run_id STRING, value INT) USING DELTA")
    spark.createDataFrame([(RUN_ID, 1)], "run_id STRING, value INT").write.mode("append").saveAsTable(table)
    count = spark.table(table).where(f"run_id = '{RUN_ID}'").count()
    if count != 1:
        raise RuntimeError("DiagnosticDeltaReadbackMismatch")
    return {"table": table, "matchingRows": count}


stage("delta_write_readback", delta)
stage("function_dns", lambda: {"addresses": sorted({r[4][0] for r in socket.getaddrinfo(
    "esehspark-score-api.azurewebsites.net", 443, type=socket.SOCK_STREAM)})})
stage("keyvault_dns", lambda: {"addresses": sorted({r[4][0] for r in socket.getaddrinfo(
    "esehsparkkv.vault.azure.net", 443, type=socket.SOCK_STREAM)})})


def kafka_dns():
    connection = dict(part.split("=", 1) for part in
                      secrets["eventstream-destination-connection"].split(";") if "=" in part)
    host = urlsplit(connection["Endpoint"]).hostname
    addresses = sorted({r[4][0] for r in socket.getaddrinfo(host, 9093, type=socket.SOCK_STREAM)})
    return {"host": host, "addresses": addresses,
            "allPrivate": bool(addresses) and all(ipaddress.ip_address(ip).is_private for ip in addresses),
            "kafkaConnectionAttempted": False}


def private_function():
    host = "esehspark-score-api.azurewebsites.net"
    addresses = {r[4][0] for r in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)}
    if not addresses or not all(ipaddress.ip_address(ip).is_private for ip in addresses):
        raise RuntimeError("FunctionRouteNotPrivate")
    payload = {"run_id": RUN_ID, "message_id": RUN_ID, "equipment_id": 17,
               "timestamp": datetime.now(timezone.utc).isoformat(),
               "calculated_temperature": 82.5}
    request = urllib.request.Request(
        f"https://{host}/api/score", data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json",
                 "x-functions-key": secrets["score-api-function-key"],
                 "Idempotency-Key": f"{RUN_ID}:{RUN_ID}"}, method="POST")
    with urllib.request.urlopen(request, timeout=15) as response:
        body = json.load(response)
        score = body.get("score")
        if (response.status not in (200, 201) or isinstance(score, bool)
                or not isinstance(score, int) or not 1 <= score <= 100):
            raise RuntimeError("PrivateScoringResponseMismatch")
        return {"httpStatus": response.status, "score": body["score"],
                "privateAddresses": sorted(addresses), "tlsVerified": True}


stage("kafka_dns_only", kafka_dns)
stage("private_function_scoring", private_function)


def external_network_probe():
    try:
        with urllib.request.urlopen("https://example.com", timeout=5) as response:
            response.read(1)
    except TimeoutError:
        return {"endpoint": "https://example.com", "transportFailureObserved": True,
                "errorType": "TimeoutError", "policyAttribution": "Not independently established"}
    except urllib.error.URLError as error:
        if not isinstance(error.reason, OSError):
            raise
        return {"endpoint": "https://example.com", "transportFailureObserved": True,
                "errorType": type(error.reason).__name__,
                "policyAttribution": "Not independently established"}
    raise RuntimeError("UnapprovedExternalEndpointReachable")


if PROBE_EXTERNAL_BLOCK:
    stage("unapproved_external_endpoint", external_network_probe)
failed = [result["stage"] + ":" + result["errorType"] for result in results if result["status"] == "Failed"]
notebookutils.notebook.exit(json.dumps({
    "diagnosticOnly": True,
    "allDiagnosticStagesSucceeded": not failed,
    "privateKafkaRouteProven": False,
    "acceptancePassed": False,
    "results": results,
}, sort_keys=True))
