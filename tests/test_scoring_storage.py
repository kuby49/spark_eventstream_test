from concurrent.futures import ThreadPoolExecutor
import importlib.util
from pathlib import Path
import sys
from threading import Barrier, Lock
import types
import unittest
from unittest.mock import Mock, patch

from src.scoring_api import logic


class StorageRandomScoreTests(unittest.TestCase):
    def setUp(self):
        class ExistsError(Exception):
            pass
        class MissingError(Exception):
            pass
        self.exists, self.missing = ExistsError, MissingError
        exceptions = types.ModuleType("azure.core.exceptions")
        exceptions.ResourceExistsError, exceptions.ResourceNotFoundError = ExistsError, MissingError
        spec = importlib.util.spec_from_file_location(
            "storage_test", Path(__file__).resolve().parents[1] / "src/scoring_api/storage.py")
        self.storage = importlib.util.module_from_spec(spec)
        self.modules = patch.dict(sys.modules, {"logic": logic, "azure.core.exceptions": exceptions})
        self.modules.start()
        self.addCleanup(self.modules.stop)
        spec.loader.exec_module(self.storage)
        self.repo = self.storage.AzureTableScoreRepository()

    def test_existing_score_is_returned_without_rng_or_write(self):
        client = Mock()
        client.get_entity.return_value = {
            "PartitionKey": "run", "RowKey": "message", "payloadHash": "hash", "score": 17}
        rng = Mock(side_effect=AssertionError("Must not redraw"))
        with patch.object(self.storage, "_table_client", return_value=client):
            record, created = self.repo.get_or_create("run", "message", "hash", rng)
        self.assertEqual(record.score, 17)
        self.assertFalse(created)
        client.create_entity.assert_not_called()

    def test_concurrent_conditional_insert_returns_winning_random_value_to_both(self):
        barrier, lock = Barrier(2), Lock()
        saved = {}
        missing = self.missing
        exists = self.exists
        class Table:
            def get_entity(self, **kwargs):
                with lock:
                    if saved:
                        return dict(saved)
                barrier.wait(timeout=5)
                raise missing()
            def create_entity(self, entity):
                with lock:
                    if saved:
                        raise exists()
                    saved.update(entity)
        client = Table()
        with patch.object(self.storage, "_table_client", return_value=client):
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(self.repo.get_or_create, "run", "message", "hash",
                                       lambda value=value: value) for value in (8, 94)]
                results = [future.result(timeout=10) for future in futures]
        self.assertEqual(len({record.score for record, _ in results}), 1)
        self.assertIn(results[0][0].score, (8, 94))
        self.assertEqual(sum(created for _, created in results), 1)

    def test_invalid_random_value_never_persists(self):
        client = Mock()
        client.get_entity.side_effect = self.missing()
        with patch.object(self.storage, "_table_client", return_value=client):
            for value in (0, 101, True, 3.5):
                with self.assertRaises(RuntimeError):
                    self.repo.get_or_create("run", "message", "hash", lambda: value)
        client.create_entity.assert_not_called()

    def test_invalid_stored_score_fails_without_redrawing_or_rewriting(self):
        client = Mock()
        rng = Mock(side_effect=AssertionError("Must not repair stored records silently"))
        with patch.object(self.storage, "_table_client", return_value=client):
            for value in (0, 101, True, 2.5):
                client.get_entity.return_value = {
                    "PartitionKey": "run", "RowKey": "message",
                    "payloadHash": "hash", "score": value}
                with self.assertRaises(RuntimeError):
                    self.repo.get_or_create("run", "message", "hash", rng)
        client.create_entity.assert_not_called()
        rng.assert_not_called()
