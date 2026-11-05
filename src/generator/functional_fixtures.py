"""Send two identifiable private-source fixtures; no baseline or connector changes."""

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import socket
import ssl
import urllib.parse
import urllib.request
import uuid

from private_source_canary import managed_identity_token, private_dns_guard


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--high-only", action="store_true")
    parser.add_argument("--message-id")
    parser.add_argument("--equipment-id", type=int, default=17)
    args = parser.parse_args()
    if not 0 <= args.equipment_id <= 100:
        parser.error("equipment-id must be 0..100")
    if args.message_id and not args.high_only:
        parser.error("message-id requires high-only")
    report = {"run_id": str(uuid.UUID(args.run_id)), "oapEnabled": False,
              "baselineRun": False, "brokerResolutions": [], "events": [], "allAcknowledged": False}
    logging.getLogger("kafka").setLevel(logging.CRITICAL)
    try:
        vault = "esehsparkkv.vault.azure.net"
        if {item[4][0] for item in socket.getaddrinfo(vault, 443)} != {"10.74.2.4"}:
            raise RuntimeError("VaultPrivateDnsMismatch")
        request = urllib.request.Request(
            f"https://{vault}/secrets/eventstream-source-connection?api-version=7.4",
            headers={"Authorization": "Bearer " + managed_identity_token("https://vault.azure.net")})
        with urllib.request.urlopen(request, timeout=15) as response:
            connection = json.load(response)["value"]
        fields = dict(part.split("=", 1) for part in connection.split(";") if "=" in part)
        host = urllib.parse.urlsplit(fields["Endpoint"]).hostname
        if (host != "esehusw3qzfnf3vqakins22j.servicebus.windows.net"
                or fields["EntityPath"] != "esehusw3qzfnf3vqakins22j_eh"):
            raise RuntimeError("SourceIdentityMismatch")
        report["sourceHost"] = host
        report["sourceTopic"] = fields["EntityPath"]
        from kafka import KafkaProducer
        with private_dns_guard(report["brokerResolutions"]):
            producer = KafkaProducer(
                bootstrap_servers=f"{host}:9093", security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN", sasl_plain_username="$ConnectionString",
                sasl_plain_password=connection, ssl_context=ssl.create_default_context(),
                acks="all", retries=0, max_block_ms=15000, request_timeout_ms=10000,
                api_version_auto_timeout_ms=5000)
            try:
                report["advertisedBrokers"] = [
                    {"host": broker.host, "port": broker.port}
                    for broker in producer._metadata.brokers()]
                for temperature in ((95.0,) if args.high_only else (20.0, 95.0)):
                    payload = {
                        "message_id": str(uuid.UUID(args.message_id)) if args.message_id else str(uuid.uuid4()),
                        "run_id": report["run_id"],
                        "equipment_id": args.equipment_id, "temperature": temperature,
                        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}
                    entry = {"payload": payload, "acknowledged": False}
                    report["events"].append(entry)
                    receipt = producer.send(fields["EntityPath"], json.dumps(payload).encode()).get(timeout=15)
                    entry.update(acknowledged=True, partition=receipt.partition, offset=receipt.offset)
                report["allAcknowledged"] = True
            finally:
                producer.close(timeout=5)
    except Exception as error:
        report["errorType"] = type(error).__name__
    Path(args.output).write_text(json.dumps(report), encoding="utf-8")
    return 0 if report["allAcknowledged"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
