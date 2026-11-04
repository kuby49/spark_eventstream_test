import importlib.util
from pathlib import Path
import socket
import unittest
from unittest.mock import patch


SOURCE = Path(__file__).resolve().parents[1] / "src" / "generator" / "private_source_canary.py"
spec = importlib.util.spec_from_file_location("private_source_canary", SOURCE)
canary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(canary)


class PrivateSourceCanaryTests(unittest.TestCase):
    def test_private_addresses_preserve_original_resolution_and_restore_resolver(self):
        records = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.74.2.15", 9093))]
        observed = []
        with patch("socket.getaddrinfo", return_value=records) as original:
            with canary.private_dns_guard(observed):
                self.assertEqual(socket.getaddrinfo("source.example", 9093), records)
            self.assertIs(socket.getaddrinfo, original)
        self.assertTrue(observed[0]["allPrivate"])

    def test_public_advertised_broker_is_rejected_without_rewriting_dns(self):
        records = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("20.150.246.47", 9093))]
        observed = []
        with patch("socket.getaddrinfo", return_value=records) as original:
            with canary.private_dns_guard(observed):
                with self.assertRaisesRegex(RuntimeError, "PublicBrokerResolutionRefused"):
                    socket.getaddrinfo("advertised.example", 9093)
            self.assertIs(socket.getaddrinfo, original)
        self.assertFalse(observed[0]["allPrivate"])


if __name__ == "__main__":
    unittest.main()
