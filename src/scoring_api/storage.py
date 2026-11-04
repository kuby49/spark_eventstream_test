"""Azure Table Storage adapter using the Function's managed identity."""

from __future__ import annotations

from functools import lru_cache
import os
from typing import Callable

from logic import ScoreRecord


@lru_cache(maxsize=1)
def _table_client():
    from azure.data.tables import TableClient
    from azure.identity import DefaultAzureCredential

    account_name = os.environ.get("TABLE_STORAGE_ACCOUNT")
    table_name = os.environ.get("IDEMPOTENCY_TABLE", "ScoreRequests")
    if not account_name:
        raise RuntimeError("TABLE_STORAGE_ACCOUNT is not configured")
    return TableClient(
        endpoint=f"https://{account_name}.table.core.windows.net",
        table_name=table_name,
        credential=DefaultAzureCredential(exclude_interactive_browser_credential=True),
    )


class AzureTableScoreRepository:
    @staticmethod
    def _record(existing):
        try:
            score = existing["score"]
            if isinstance(score, bool) or not isinstance(score, int) or not 1 <= score <= 100:
                raise ValueError("InvalidStoredScore")
            return ScoreRecord(
                run_id=existing["PartitionKey"], message_id=existing["RowKey"],
                payload_hash=existing["payloadHash"], score=score)
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("Stored idempotency record is invalid") from exc

    def get_or_create(
        self, run_id: str, message_id: str, payload_hash: str,
        score_factory: Callable[[], int],
    ) -> tuple[ScoreRecord, bool]:
        from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

        client = _table_client()
        try:
            return self._record(client.get_entity(partition_key=run_id, row_key=message_id)), False
        except ResourceNotFoundError:
            pass
        score = score_factory()
        if isinstance(score, bool) or not isinstance(score, int) or not 1 <= score <= 100:
            raise RuntimeError("Generated score is outside the contract range")
        record = ScoreRecord(run_id=run_id, message_id=message_id,
                             payload_hash=payload_hash, score=score)

        entity = {
            "PartitionKey": record.run_id,
            "RowKey": record.message_id,
            "payloadHash": record.payload_hash,
            "score": record.score,
        }
        try:
            client.create_entity(entity=entity)
            return record, True
        except ResourceExistsError:
            existing = client.get_entity(
                partition_key=record.run_id,
                row_key=record.message_id,
            )
            return self._record(existing), False
