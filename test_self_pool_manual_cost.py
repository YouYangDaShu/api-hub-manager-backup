import asyncio
import math
import sqlite3
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import routes
from fastapi import HTTPException


class SelfPoolManualCostTests(unittest.TestCase):
    def test_add_list_delete_and_date_totals(self):
        with tempfile.TemporaryDirectory() as td, patch.object(routes, "SELF_POOL_COST_FILE", Path(td) / "costs.json"):
            today = date.today()
            first = asyncio.run(routes.add_manual_cost("self-pool", routes.SelfPoolCostCreate(
                amount=12.5, cost_date=today, note="CPA采购"
            )))
            asyncio.run(routes.add_manual_cost("self-pool", routes.SelfPoolCostCreate(
                amount=3.25, cost_date=today - timedelta(days=1), note="昨天成本"
            )))
            result = asyncio.run(routes.list_manual_costs("self-pool"))
            self.assertEqual(len(result["data"]), 2)
            self.assertEqual(result["totals"]["today_cost"], 12.5)
            self.assertEqual(result["totals"]["yesterday_cost"], 3.25)
            self.assertEqual(result["totals"]["recent_cost"], 15.75)
            self.assertEqual(result["totals"]["total_cost"], 15.75)

            asyncio.run(routes.delete_manual_cost("self-pool", first["data"]["id"]))
            after = routes._self_pool_summary()
            self.assertEqual(after["today_cost"], 0.0)
            self.assertEqual(after["total_cost"], 3.25)

    def test_invalid_amount_and_wrong_account_are_rejected(self):
        with tempfile.TemporaryDirectory() as td, patch.object(routes, "SELF_POOL_COST_FILE", Path(td) / "costs.json"):
            for value in (0, -1, math.inf):
                with self.assertRaises(HTTPException):
                    asyncio.run(routes.add_manual_cost("self-pool", routes.SelfPoolCostCreate(
                        amount=value, cost_date=date.today(), note=""
                    )))
            with self.assertRaises(HTTPException):
                asyncio.run(routes.add_manual_cost("real-account", routes.SelfPoolCostCreate(
                    amount=1, cost_date=date.today(), note=""
                )))

    def test_manual_cost_is_used_in_self_pool_profit(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "one-api.db"
            ownership_path = root / "channel_ownership.json"
            conn = sqlite3.connect(db_path)
            conn.executescript("""
                CREATE TABLE options (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE channels (id INTEGER PRIMARY KEY, key TEXT);
                CREATE TABLE logs (type INTEGER, channel_id INTEGER, quota INTEGER, created_at INTEGER);
                INSERT INTO options VALUES ('QuotaPerUnit', '1000000');
                INSERT INTO channels VALUES (145, 'sk-self');
                INSERT INTO logs VALUES (2, 145, 10000000, strftime('%s', 'now'));
            """)
            conn.commit()
            conn.close()
            ownership_path.write_text(
                '{"145":{"channel_id":"145","owner_account_id":"self-pool"}}', encoding="utf-8"
            )
            with patch.object(routes, "SELF_POOL_COST_FILE", root / "costs.json"), \
                 patch.object(routes, "SITE_BILLING_DB", db_path), \
                 patch.object(routes, "CHANNEL_OWNERSHIP_FILE", ownership_path), \
                 patch.object(routes, "_load_revenue_adjustments", return_value={}):
                asyncio.run(routes.add_manual_cost("self-pool", routes.SelfPoolCostCreate(
                    amount=4.0, cost_date=date.today(), note=""
                )))
                summaries = [routes._self_pool_summary()]
                routes._attach_site_revenue(summaries)
            summary = summaries[0]
            self.assertEqual(summary["site_revenue"], 10.0)
            self.assertEqual(summary["today_cost"], 4.0)
            self.assertEqual(summary["site_profit"], 6.0)
            self.assertEqual(summary["site_profit_total"], 6.0)


if __name__ == "__main__":
    unittest.main()
