import unittest
from unittest.mock import Mock, patch
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from src.scoring_api.logic import (
    ContractError,
    PayloadConflict,
    ScoreRecord,
    generate_score,
    score_request,
)


class MemoryRepository:
    def __init__(self):
        self.records = {}
        self.lock = Lock()

    def get_or_create(self, run_id, message_id, payload_hash, score_factory):
        with self.lock:
            key = (run_id, message_id)
            if key in self.records:
                return self.records[key], False
            record = ScoreRecord(run_id, message_id, payload_hash, score_factory())
            self.records[key] = record
            return record, True


class ScoringContractTests(unittest.TestCase):
    def setUp(self):
        self.run_id = str(uuid4())
        self.message_id = str(uuid4())
        self.payload = {
            "message_id": self.message_id,
            "run_id": self.run_id,
            "equipment_id": 25,
            "timestamp": "2026-11-02T12:00:00Z",
            "calculated_temperature": 83.5,
        }
        self.key = f"{self.run_id}:{self.message_id}"

    def test_random_score_inclusive_bounds_with_controlled_rng(self):
        with patch("src.scoring_api.logic.secrets.randbelow", side_effect=[0, 99]) as rng:
            self.assertEqual(generate_score(), 1)
            self.assertEqual(generate_score(), 100)
        self.assertEqual(rng.call_args_list[0].args, (100,))

    def test_same_temperature_can_have_different_valid_random_scores(self):
        self.assertEqual(score_request(self.payload, self.key, MemoryRepository(), lambda: 7)[0].score, 7)
        self.assertEqual(score_request(self.payload, self.key, MemoryRepository(), lambda: 92)[0].score, 92)

    def test_concurrent_requests_converge_without_redrawing_persisted_score(self):
        repository = MemoryRepository()
        rng = Mock(return_value=37)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: score_request(self.payload, self.key, repository, rng), range(16)))
        self.assertEqual({row.score for row, _ in results}, {37})
        self.assertEqual(sum(created for _, created in results), 1)
        rng.assert_called_once()

    def test_repeated_request_returns_persisted_score(self):
        repository = MemoryRepository()
        first, created = score_request(self.payload, self.key, repository)
        second, created_again = score_request(self.payload, self.key, repository)
        self.assertTrue(created)
        self.assertFalse(created_again)
        self.assertEqual(first, second)
        self.assertIsInstance(first, ScoreRecord)

    def test_same_idempotency_key_with_different_payload_conflicts(self):
        repository = MemoryRepository()
        score_request(self.payload, self.key, repository)
        changed = dict(self.payload, calculated_temperature=90.0)
        with self.assertRaises(PayloadConflict):
            score_request(changed, self.key, repository)

    def test_rejects_non_utc_timestamp(self):
        changed = dict(self.payload, timestamp="2026-11-02T12:00:00-07:00")
        with self.assertRaises(ContractError):
            score_request(changed, self.key, MemoryRepository())

    def test_rejects_invalid_idempotency_key(self):
        with self.assertRaises(ContractError):
            score_request(self.payload, "wrong", MemoryRepository())

    def test_rejects_out_of_range_equipment_id(self):
        changed = dict(self.payload, equipment_id=101)
        with self.assertRaises(ContractError):
            score_request(changed, self.key, MemoryRepository())

    def test_rejects_non_finite_temperature(self):
        changed = dict(self.payload, calculated_temperature=float("nan"))
        with self.assertRaises(ContractError):
            score_request(changed, self.key, MemoryRepository())


if __name__ == "__main__":
    unittest.main()
