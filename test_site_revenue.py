import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import routes


class SiteRevenueTests(unittest.TestCase):
    def test_huiliu_channel_71_moves_to_secondary_for_20260827_today_only(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "one-api.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE options (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE channels (id INTEGER PRIMARY KEY, key TEXT);
                CREATE TABLE logs (type INTEGER, channel_id INTEGER, quota INTEGER, created_at INTEGER);
                INSERT INTO options VALUES ('QuotaPerUnit', '1000000');
                INSERT INTO channels VALUES (56, 'sk-secondary');
                INSERT INTO channels VALUES (71, 'sk-primary-multi');
                INSERT INTO logs VALUES (2, 56, 3000000, 1787763600);
                INSERT INTO logs VALUES (2, 71, 11000000, 1787763600);
                INSERT INTO logs VALUES (2, 71, 9000000, 1787677200);
                """
            )
            conn.commit()
            conn.close()
            summaries = [
                {"id": "b0d14b19", "upstream_key": "sk-secondary", "today_cost": 10.0, "total_cost": 100.0},
                {"id": "6d0226c3", "upstream_key": "sk-primary", "today_cost": 2.0, "total_cost": 200.0},
            ]
            fixed_now = datetime.fromtimestamp(1787781600).astimezone()
            with patch.object(routes, "SITE_BILLING_DB", db_path), \
                 patch.object(routes, "_site_revenue_now", return_value=fixed_now), \
                 patch.object(routes, "CHANNEL_OWNERSHIP_FILE", Path(td) / "missing-ownership.json"), \
                 patch.object(routes, "_load_revenue_adjustments", return_value={}):
                routes._attach_site_revenue(summaries)

            self.assertEqual(summaries[0]["site_revenue"], 14.0)
            self.assertEqual(summaries[1]["site_revenue"], 0.0)
            self.assertEqual(summaries[0]["site_revenue_total"], 3.0)
            self.assertEqual(summaries[1]["site_revenue_total"], 20.0)
            self.assertIn("仅2026-08-27", summaries[0]["site_revenue_status"])

    def test_revenue_is_attributed_by_exact_upstream_key(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "one-api.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE options (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE channels (id INTEGER PRIMARY KEY, key TEXT);
                CREATE TABLE logs (type INTEGER, channel_id INTEGER, quota INTEGER, created_at INTEGER);
                INSERT INTO options VALUES ('QuotaPerUnit', '1000000');
                INSERT INTO channels VALUES (1, 'sk-sy');
                INSERT INTO channels VALUES (2, 'sk-other');
                INSERT INTO channels VALUES (63, 'sk-mobius-pro');
                INSERT INTO channels VALUES (64, 'sk-mobius-plus');
                INSERT INTO logs VALUES (2, 1, 8502600, strftime('%s', 'now'));
                INSERT INTO logs VALUES (2, 2, 9900000, strftime('%s', 'now'));
                INSERT INTO logs VALUES (2, 63, 28030300, strftime('%s', 'now'));
                INSERT INTO logs VALUES (2, 64, 1000000, strftime('%s', 'now'));
                """
            )
            conn.commit()
            conn.close()

            summaries = [
                {"id": "unmapped-key-account", "upstream_key": "sk-sy", "today_cost": 5.8651},
                {"id": "926d2a81", "upstream_key": "sk-mobius-plus", "today_cost": 27.2582},
                {"id": "missing", "upstream_key": "sk-missing", "today_cost": 2.0},
            ]
            with patch.object(routes, "SITE_BILLING_DB", db_path), \
                 patch.object(routes, "CHANNEL_OWNERSHIP_FILE", Path(td) / "missing-ownership.json"):
                routes._attach_site_revenue(summaries)

            self.assertEqual(summaries[0]["site_revenue"], 8.5026)
            self.assertEqual(summaries[0]["site_profit"], 2.6375)
            self.assertEqual(summaries[0]["site_revenue_status"], "已按上游Key匹配")
            self.assertEqual(summaries[1]["site_revenue"], 29.0303)
            self.assertEqual(summaries[1]["site_profit"], 1.7721)
            self.assertEqual(summaries[1]["site_revenue_status"], "已按本站渠道ID匹配 3 条")
            self.assertEqual(summaries[2]["site_revenue"], 0.0)
            self.assertEqual(summaries[2]["site_profit"], -2.0)

    def test_dc_uses_exact_production_channel_mapping(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "one-api.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE options (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE channels (id INTEGER PRIMARY KEY, key TEXT);
                CREATE TABLE logs (type INTEGER, channel_id INTEGER, quota INTEGER, created_at INTEGER);
                INSERT INTO options VALUES ('QuotaPerUnit', '1000000');
                INSERT INTO channels VALUES (3, 'sk-other-dcvx-key');
                INSERT INTO channels VALUES (98, 'sk-dc-exact-key');
                INSERT INTO logs VALUES (2, 3, 362808222, strftime('%s', 'now'));
                INSERT INTO logs VALUES (2, 98, 1817520, strftime('%s', 'now'));
                """
            )
            conn.commit()
            conn.close()
            summaries = [{
                "id": "7d5b654c",
                "upstream_key": "sk-dc-exact-key",
                "today_cost": 0.0285,
                "total_cost": 3.696,
            }]
            with patch.object(routes, "SITE_BILLING_DB", db_path), \
                 patch.object(routes, "CHANNEL_OWNERSHIP_FILE", Path(td) / "missing-ownership.json"):
                routes._attach_site_revenue(summaries)

            self.assertEqual(summaries[0]["site_revenue_total"], 1.8175)
            self.assertEqual(summaries[0]["site_profit_total"], -1.8785)
    def test_historical_revenue_adjustment_is_idempotent(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "one-api.db"
            adjustment_path = Path(td) / "site_revenue_adjustments.json"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE options (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE channels (id INTEGER PRIMARY KEY, key TEXT);
                CREATE TABLE logs (type INTEGER, channel_id INTEGER, quota INTEGER, created_at INTEGER);
                INSERT INTO options VALUES ('QuotaPerUnit', '1000000');
                INSERT INTO channels VALUES (79, 'sk-liangjie');
                INSERT INTO logs VALUES (2, 79, 316228400, strftime('%s', 'now'));
                """
            )
            conn.commit()
            conn.close()
            adjustment_path.write_text(
                '{"bb065c1c": {"historical_revenue": 240.9}}',
                encoding="utf-8",
            )
            summaries = [{
                "id": "bb065c1c",
                "upstream_key": "sk-liangjie",
                "today_cost": 0.0,
                "total_cost": 583.9141,
            }]
            with patch.object(routes, "SITE_BILLING_DB", db_path), \
                 patch.object(routes, "SITE_REVENUE_ADJUSTMENTS", adjustment_path), \
                 patch.object(routes, "CHANNEL_OWNERSHIP_FILE", Path(td) / "missing-ownership.json"):
                routes._attach_site_revenue(summaries)
                first = summaries[0]["site_revenue_total"]
                routes._attach_site_revenue(summaries)
                second = summaries[0]["site_revenue_total"]
            self.assertEqual(first, 557.1284)
            self.assertEqual(second, first)
            self.assertEqual(summaries[0]["site_profit_total"], -26.7857)

    def test_self_pool_owns_unmapped_channel_revenue(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "one-api.db"
            ownership_path = Path(td) / "channel_ownership.json"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE options (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE channels (id INTEGER PRIMARY KEY, key TEXT);
                CREATE TABLE logs (type INTEGER, channel_id INTEGER, quota INTEGER, created_at INTEGER);
                INSERT INTO options VALUES ('QuotaPerUnit', '1000000');
                INSERT INTO channels VALUES (145, 'sk-self-cpa');
                INSERT INTO logs VALUES (2, 145, 2500000, strftime('%s', 'now'));
                """
            )
            conn.commit()
            conn.close()
            ownership_path.write_text(
                '{"145": {"channel_id": "145", "owner_account_id": "self-pool"}}',
                encoding="utf-8",
            )
            summaries = [{
                "id": "926d2a81",
                "upstream_key": "sk-other",
                "today_cost": 1.0,
                "total_cost": 10.0,
            }]
            with patch.object(routes, "SITE_BILLING_DB", db_path), \
                 patch.object(routes, "CHANNEL_OWNERSHIP_FILE", ownership_path), \
                 patch.object(routes, "SELF_POOL_COST_FILE", Path(td) / "self-pool-costs.json"), \
                 patch.object(routes, "_load_revenue_adjustments", return_value={}):
                totals = routes._attach_site_revenue(summaries)
            pool = next(item for item in summaries if item["id"] == "self-pool")
            self.assertEqual(pool["site_revenue"], 2.5)
            self.assertEqual(pool["site_revenue_total"], 2.5)
            self.assertEqual(pool["site_profit"], 2.5)
            self.assertIn("手动归属", pool["site_revenue_status"])
            self.assertEqual(summaries[0]["site_revenue"], 0.0)
            self.assertEqual(totals["matched_today"], 2.5)
            self.assertEqual(totals["site_today_revenue"], 2.5)

    def test_key_matching_does_not_double_count_already_claimed_channel(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "one-api.db"
            ownership_path = Path(td) / "channel_ownership.json"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE options (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE channels (id INTEGER PRIMARY KEY, key TEXT);
                CREATE TABLE logs (type INTEGER, channel_id INTEGER, quota INTEGER, created_at INTEGER);
                INSERT INTO options VALUES ('QuotaPerUnit', '1000000');
                INSERT INTO channels VALUES (128, 'sk-wawa-shared-key');
                INSERT INTO logs VALUES (2, 128, 40000000, strftime('%s', 'now'));
                """
            )
            conn.commit()
            conn.close()
            # 渠道 128 手动归属给 wawa-main
            ownership_path.write_text(
                '{"128": {"channel_id": "128", "owner_account_id": "wawa-main"}}',
                encoding="utf-8",
            )
            summaries = [
                {"id": "wawa-main", "upstream_key": "sk-main", "today_cost": 30.0},
                {"id": "wawa-sub", "upstream_key": "sk-wawa-shared-key", "today_cost": 10.0},
            ]
            with patch.object(routes, "SITE_BILLING_DB", db_path), \
                 patch.object(routes, "CHANNEL_OWNERSHIP_FILE", ownership_path), \
                 patch.object(routes, "SITE_CHANNEL_IDS_BY_ACCOUNT", {}), \
                 patch.object(routes, "_load_revenue_adjustments", return_value={}):
                totals = routes._attach_site_revenue(summaries)

            # 主号按 ID 认领 40.0
            self.assertEqual(summaries[0]["site_revenue"], 40.0)
            self.assertIn("手动归属", summaries[0]["site_revenue_status"])
            # 副号虽然持有相同 Key，但渠道已被认领，绝不重复计算
            self.assertEqual(summaries[1]["site_revenue"], 0.0)
            self.assertIn("已按ID归属其他账号", summaries[1]["site_revenue_status"])
            # 全站总收入和归属收入精确相等，不发生双计
            self.assertEqual(totals["matched_today"], 40.0)
            self.assertEqual(totals["site_today_revenue"], 40.0)


