import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import routes


class UsageLedgerTests(unittest.TestCase):
    def test_upstream_reset_does_not_reduce_local_total(self):
        with tempfile.TemporaryDirectory() as td:
            ledger_file = Path(td) / "usage_ledger.json"
            with patch.object(routes, "USAGE_LEDGER_FILE", ledger_file):
                first = [{"id": "sy", "total_cost": 35.294}]
                routes._attach_usage_ledger(first)
                self.assertEqual(first[0]["total_cost"], 35.294)

                reset = [{"id": "sy", "total_cost": 0.0}]
                routes._attach_usage_ledger(reset)
                self.assertEqual(reset[0]["total_cost"], 35.294)
                self.assertTrue(reset[0]["usage_reset"])

                grown = [{"id": "sy", "total_cost": 2.5}]
                routes._attach_usage_ledger(grown)
                self.assertEqual(grown[0]["total_cost"], 37.794)

                saved = json.loads(ledger_file.read_text(encoding="utf-8"))
                self.assertEqual(saved["sy"]["total_cost"], 37.794)

    def test_reset_account_uses_current_upstream_total(self):
        with tempfile.TemporaryDirectory() as td:
            ledger_file = Path(td) / "usage_ledger.json"
            ledger_file.write_text(json.dumps({"ebd3907b": {
                "total_cost": 343.0423,
                "last_upstream_total": 32.562,
                "reset_count": 10,
            }}), encoding="utf-8")
            with patch.object(routes, "USAGE_LEDGER_FILE", ledger_file):
                current = [{"id": "ebd3907b", "total_cost": 32.562}]
                routes._attach_usage_ledger(current)
                self.assertEqual(current[0]["total_cost"], 32.562)
                saved = json.loads(ledger_file.read_text(encoding="utf-8"))
                self.assertEqual(saved["ebd3907b"]["reset_count"], 0)
    def test_daily_snapshot_does_not_reduce_after_log_cleanup(self):
        with tempfile.TemporaryDirectory() as td:
            history_file = Path(td) / "usage_history.json"
            with patch.object(routes, "USAGE_HISTORY_FILE", history_file):
                first = [{"id": "sy", "today_cost": 13.1293}]
                routes._attach_usage_history(first)
                reset = [{"id": "sy", "today_cost": 0.0}]
                routes._attach_usage_history(reset)
                self.assertEqual(reset[0]["today_cost"], 13.1293)


if __name__ == "__main__":
    unittest.main()
