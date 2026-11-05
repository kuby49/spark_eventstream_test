"""Pure scoring contract and idempotency logic."""

from __future__ import annotations

import hashlib
import json
import math
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Protocol
from uuid import UUID

_UTC_TIMESTAMP = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|\+00:00)$")


class ContractError(ValueError):
    pass


class PayloadConflict(ValueError):
    pass


@dataclass(frozen=True)
class ScoreRecord:
    run_id: str
    message_id: str
    payload_hash: str
    score: int


class ScoreRepository(Protocol):
    def get_or_create(
        self, run_id: str, message_id: str, payload_hash: str,
        score_factory: Callable[[], int],
    ) -> tuple[ScoreRecord, bool]:
        """Return the existing score or conditionally persist a new random score."""


def _canonical_uuid(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ContractError(f"{field} must be a UUID string")
    try:
        return str(UUID(value))
    except (ValueError, AttributeError) as exc:
        raise ContractError(f"{field} must be a UUID string") from exc


def validate_request(payload: object, idempotency_key: object) -> tuple[dict, str, str, str]:
    if not isinstance(payload, dict):
        raise ContractError("The request body must be a JSON object")

    required = {
        "message_id",
        "run_id",
        "equipment_id",
        "timestamp",
        "calculated_temperature",
    }
    unexpected = payload.keys() - required
    if unexpected:
        raise ContractError(f"Unexpected field: {sorted(unexpected)[0]}")
    missing = required.difference(payload)
    if missing:
        raise ContractError(f"Missing required field: {sorted(missing)[0]}")

    message_id = _canonical_uuid(payload["message_id"], "message_id")
    run_id = _canonical_uuid(payload["run_id"], "run_id")
    equipment_id = payload["equipment_id"]
    if isinstance(equipment_id, bool) or not isinstance(equipment_id, int):
        raise ContractError("equipment_id must be an integer")
    if not 0 <= equipment_id <= 100:
        raise ContractError("equipment_id must be between 0 and 100")

    timestamp = payload["timestamp"]
    if not isinstance(timestamp, str) or not _UTC_TIMESTAMP.fullmatch(timestamp):
        raise ContractError("timestamp must be an RFC3339 UTC timestamp")
    try:
        parsed_timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError("timestamp must be an RFC3339 UTC timestamp") from exc
    if parsed_timestamp.utcoffset() != timezone.utc.utcoffset(parsed_timestamp):
        raise ContractError("timestamp must use UTC")

    temperature = payload["calculated_temperature"]
    if isinstance(temperature, bool) or not isinstance(temperature, (int, float)):
        raise ContractError("calculated_temperature must be a finite number")
    if not math.isfinite(float(temperature)) or not 0 <= float(temperature) <= 100:
        raise ContractError("calculated_temperature must be finite and between 0 and 100")

    expected_key = f"{run_id}:{message_id}"
    if not isinstance(idempotency_key, str) or idempotency_key != expected_key:
        raise ContractError("Idempotency-Key must be run_id:message_id")

    canonical = {
        "message_id": message_id,
        "run_id": run_id,
        "equipment_id": equipment_id,
        "timestamp": parsed_timestamp.astimezone(timezone.utc).isoformat(),
        "calculated_temperature": float(temperature),
    }
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    ).hexdigest()
    return canonical, run_id, message_id, digest


def generate_score() -> int:
    return secrets.randbelow(100) + 1


def score_request(
    payload: object,
    idempotency_key: object,
    repository: ScoreRepository,
    score_factory: Callable[[], int] = generate_score,
) -> tuple[ScoreRecord, bool]:
    _, run_id, message_id, payload_hash = validate_request(payload, idempotency_key)
    persisted, created = repository.get_or_create(run_id, message_id, payload_hash, score_factory)
    if persisted.payload_hash != payload_hash:
        raise PayloadConflict("Idempotency-Key was already used with a different payload")
    if isinstance(persisted.score, bool) or not isinstance(persisted.score, int) or not 1 <= persisted.score <= 100:
        raise RuntimeError("Persisted score is outside the contract range")
    return persisted, created
