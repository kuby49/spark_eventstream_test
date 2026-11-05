"""One-event Kafka canary that refuses every public broker resolution."""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import ipaddress
import json
import logging
import socket
import ssl
import urllib.parse
import urllib.request
import uuid


@contextmanager
def private_dns_guard(observed):
    original = socket.getaddrinfo

    def resolve(host, port, *args, **kwargs):
        records = original(host, port, *args, **kwargs)
        addresses = sorted({record[4][0] for record in records})
        private = bool(addresses) and all(ipaddress.ip_address(ip).is_private for ip in addresses)
        observed.append({"host": host, "port": port, "addresses": addresses, "allPrivate": private})
        if not private:
            raise RuntimeError("PublicBrokerResolutionRefused")
        return records

    socket.getaddrinfo = resolve
    try:
        yield
    finally:
        socket.getaddrinfo = original


def managed_identity_token(audience):
    resource = urllib.parse.quote(audience, safe="")
    request = urllib.request.Request(
        "http://169.254.169.254/metadata/identity/oauth2/token"
        f"?api-version=2018-02-01&resource={resource}", headers={"Metadata": "true"})
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)["access_token"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-source-ip", required=True)
    parser.add_argument("--expected-vault-ip", required=True)
    parser.add_argument("--run-id", required=True)
    arguments = parser.parse_args()
    run_id = str(uuid.UUID(arguments.run_id))
    message_id = str(uuid.uuid4())
    report = {"run_id": run_id, "message_id": message_id, "baselineRun": False,
              "acknowledged": False, "brokerResolutions": []}
    stage = "source_secret"
    logging.getLogger("kafka").setLevel(logging.CRITICAL)
    try:
        vault = "esehsparkkv.vault.azure.net"
        vault_ips = {entry[4][0] for entry in socket.getaddrinfo(vault, 443)}
        if vault_ips != {arguments.expected_vault_ip}:
            raise RuntimeError("KeyVaultPrivateEndpointMismatch")
        token = managed_identity_token("https://vault.azure.net")
        request = urllib.request.Request(
            f"https://{vault}/secrets/eventstream-source-connection?api-version=7.4",
            headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(request, timeout=15) as response:
            connection = json.load(response)["value"]
        fields = dict(part.split("=", 1) for part in connection.split(";") if "=" in part)
        host = urllib.parse.urlsplit(fields["Endpoint"]).hostname
        topic = fields["EntityPath"]
        stage = "source_dns"
        addresses = {entry[4][0] for entry in socket.getaddrinfo(host, 9093)}
        if addresses != {arguments.expected_source_ip}:
            raise RuntimeError("SourcePrivateEndpointMismatch")
        report.update({"sourceHost": host, "topic": topic, "privateAddresses": sorted(addresses)})
        stage = "kafka_authenticated_send"
        from kafka import KafkaProducer

        payload = {"message_id": message_id, "run_id": run_id, "equipment_id": 17,
                   "temperature": 20.0, "timestamp": datetime.now(timezone.utc).isoformat()}
        report["payload"] = payload
        with private_dns_guard(report["brokerResolutions"]):
            producer = KafkaProducer(
                bootstrap_servers=f"{host}:9093", security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN", sasl_plain_username="$ConnectionString",
                sasl_plain_password=connection, ssl_context=ssl.create_default_context(),
                acks="all", retries=0, max_block_ms=15000, request_timeout_ms=10000,
                api_version_auto_timeout_ms=5000)
            try:
                receipt = producer.send(topic, json.dumps(payload).encode()).get(timeout=15)
                report.update({"acknowledged": True, "partition": receipt.partition,
                               "offset": receipt.offset, "tlsVerified": True})
            finally:
                producer.close(timeout=5)
    except Exception as error:
        report.update({"failedStage": stage, "errorType": type(error).__name__})
    report["completedAtUtc"] = datetime.now(timezone.utc).isoformat()
    print("PRIVATE_SOURCE_CANARY:" + json.dumps(report, sort_keys=True))
    return 0 if report["acknowledged"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
