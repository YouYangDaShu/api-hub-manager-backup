import json
import sqlite3
import tempfile
import unittest
import asyncio
from pathlib import Path
from unittest.mock import patch

import channel_monitor
import routes


class HubFixTests(unittest.TestCase):
    def test_combination_default_order(self):
        self.assertEqual(
            channel_monitor.normalize_combination_order(None),
            list(channel_monitor.DEFAULT_COMBINATION_ORDER),
        )

    def test_combination_order_save_and_read(self):
        with tempfile.TemporaryDirectory() as td:
            settings_file = Path(td) / "settings.json"
            with patch.object(routes, "SETTINGS_FILE", settings_file):
                asyncio.run(routes.update_settings(routes.SettingsUpdate(
                    channel_combination_order=["stable", "other"],
                )))
                saved = json.loads(settings_file.read_text(encoding="utf-8"))
                expected = ["stable", "other"] + [
                    key for key in channel_monitor.DEFAULT_COMBINATION_ORDER
                    if key not in {"stable", "other"}
                ]
                self.assertEqual(saved["channel_combination_order"], expected)
                response = asyncio.run(routes.get_settings())
                self.assertEqual(response["data"]["channel_combination_order"], expected)
                self.assertEqual(
                    [item["key"] for item in response["data"]["channel_combination_options"]],
                    list(channel_monitor.DEFAULT_COMBINATION_ORDER),
                )

    def test_combination_order_rejects_duplicate_and_unknown(self):
        for value in (["stable", "stable"], ["not-a-category"]):
            with self.subTest(value=value):
                with self.assertRaises(Exception) as caught:
                    asyncio.run(routes.update_settings(routes.SettingsUpdate(
                        channel_combination_order=value,
                    )))
                self.assertEqual(caught.exception.status_code, 422)

    def test_invalid_stored_combination_order_falls_back_to_default(self):
        with tempfile.TemporaryDirectory() as td:
            settings_file = Path(td) / "settings.json"
            settings_file.write_text(json.dumps({"channel_combination_order": ["stable", "stable"]}), encoding="utf-8")
            with patch.object(routes, "SETTINGS_FILE", settings_file):
                response = asyncio.run(routes.get_settings())
            self.assertEqual(response["data"]["channel_combination_order"], list(channel_monitor.DEFAULT_COMBINATION_ORDER))

    def test_custom_combination_order_changes_channel_response(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "channels.db"
            con = sqlite3.connect(db)
            con.executescript(
                """
                CREATE TABLE channels (id INTEGER PRIMARY KEY, type INTEGER, name TEXT, status INTEGER, weight INTEGER,
                test_time INTEGER, response_time INTEGER, base_url TEXT, balance REAL, balance_updated_time INTEGER,
                models TEXT, "group" TEXT, priority INTEGER, auto_ban INTEGER, test_model TEXT);
                INSERT INTO channels VALUES (1,1,'Stable',1,1,0,0,'https://stable',0,0,'gpt','stable',1,1,'');
                INSERT INTO channels VALUES (2,1,'Other',1,1,0,0,'https://other',0,0,'gpt','misc',1,1,'');
                """
            )
            con.commit(); con.close()
            settings_file = Path(td) / "settings.json"
            custom_order = ["other", "stable"] + [
                key for key in channel_monitor.DEFAULT_COMBINATION_ORDER
                if key not in {"other", "stable"}
            ]
            settings_file.write_text(json.dumps({"channel_combination_order": custom_order}), encoding="utf-8")
            with patch.object(channel_monitor, "DB_PATH", db), patch.object(channel_monitor, "STATE_PATH", Path(td) / "state.json"), patch.object(routes, "SETTINGS_FILE", settings_file):
                rows = channel_monitor.list_channels()
            self.assertEqual([row["id"] for row in rows], [2, 1])

    def test_settings_page_contains_combination_order_controls(self):
        html = Path("templates/index.html").read_text(encoding="utf-8")
        self.assertIn("channelCombinationOrderSettings", html)
        self.assertIn("combinationOrderList", html)
        self.assertIn("draggable=\"true\"", html)
        self.assertIn("data-combination-move=\"up\"", html)
        self.assertIn("data-combination-move=\"down\"", html)

    def test_manual_owner_wins_and_clear_restores_key(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "billing.db"
            con = sqlite3.connect(db)
            con.executescript(
                """
                CREATE TABLE options (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE channels (id INTEGER PRIMARY KEY, key TEXT);
                CREATE TABLE logs (type INTEGER, channel_id INTEGER, quota INTEGER, created_at INTEGER);
                INSERT INTO options VALUES ('QuotaPerUnit', '1000000');
                INSERT INTO channels VALUES (400, 'shared-key');
                INSERT INTO logs VALUES (2, 400, 10000000, strftime('%s', 'now'));
                """
            )
            con.commit(); con.close()
            ownership = Path(td) / "owners.json"
            accounts = Path(td) / "accounts.json"
            accounts.write_text(json.dumps([
                {"id": "a", "name": "A", "upstream_key": "shared-key"},
                {"id": "b", "name": "B", "upstream_key": "other"},
            ]), encoding="utf-8")
            summaries = [
                {"id": "a", "upstream_key": "shared-key", "today_cost": 1, "total_cost": 1},
                {"id": "b", "upstream_key": "other", "today_cost": 1, "total_cost": 1},
            ]
            with patch.object(routes, "SITE_BILLING_DB", db), patch.object(routes, "CHANNEL_OWNERSHIP_FILE", ownership), patch.object(routes, "ACCOUNTS_FILE", accounts):
                routes.set_channel_ownership(400, "b")
                routes._attach_site_revenue(summaries)
                self.assertIsNone(summaries[0]["site_revenue"])
                self.assertEqual(summaries[1]["site_revenue"], 10.0)
                self.assertIsNone(summaries[0]["site_profit"])
                saved = ownership.read_text(encoding="utf-8")
                self.assertNotIn("shared-key", saved)
                self.assertNotIn("password", saved)
                routes.clear_channel_ownership(400)
                summaries = [{"id": "a", "upstream_key": "shared-key", "today_cost": 1, "total_cost": 1}]
                routes._attach_site_revenue(summaries)
                self.assertEqual(summaries[0]["site_revenue"], 10.0)

    def test_ambiguous_key_is_unowned(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "billing.db"
            con = sqlite3.connect(db)
            con.executescript(
                """
                CREATE TABLE options (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE channels (id INTEGER PRIMARY KEY, key TEXT);
                CREATE TABLE logs (type INTEGER, channel_id INTEGER, quota INTEGER, created_at INTEGER);
                INSERT INTO options VALUES ('QuotaPerUnit', '1000000');
                INSERT INTO channels VALUES (401, 'same');
                INSERT INTO logs VALUES (2, 401, 1000000, strftime('%s', 'now'));
                """
            )
            con.commit(); con.close()
            summaries = [
                {"id": "a", "upstream_key": "same", "today_cost": 2},
                {"id": "b", "upstream_key": "same", "today_cost": 3},
            ]
            with patch.object(routes, "SITE_BILLING_DB", db), patch.object(routes, "CHANNEL_OWNERSHIP_FILE", Path(td) / "owners.json"):
                routes._attach_site_revenue(summaries)
            self.assertIsNone(summaries[0]["site_profit"])
            self.assertIsNone(summaries[1]["site_profit"])

    def test_combination_detection_and_server_sort(self):
        self.assertEqual(channel_monitor.channel_combination_rank({"group": "稳定", "name": "GPT-PRO", "models": [], "test_model": ""}), 1)
        self.assertEqual(channel_monitor.channel_combination_rank({"group": "stable P20", "name": "GPT pro20x 号池", "models": [], "test_model": ""}), 2)
        self.assertEqual(channel_monitor.channel_combination_rank({"group": "stable", "name": "profile", "models": [], "test_model": ""}), 4)
        self.assertEqual(channel_monitor.classify_combination({"group": "stable", "name": "GPT pro20x 号池", "models": [], "test_model": ""}), "pro_stable_p20")
        self.assertEqual(channel_monitor.classify_combination({"group": "stable", "name": "GPT-PRO 混合", "models": [], "test_model": ""}), "pro_stable_mixed")
        self.assertEqual(channel_monitor.classify_combination({"group": "stable", "name": "profile", "models": [], "test_model": ""}), "stable")
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "channels.db"
            con = sqlite3.connect(db)
            con.executescript(
                """
                CREATE TABLE channels (id INTEGER PRIMARY KEY, type INTEGER, name TEXT, status INTEGER, weight INTEGER,
                test_time INTEGER, response_time INTEGER, base_url TEXT, balance REAL, balance_updated_time INTEGER,
                models TEXT, "group" TEXT, priority INTEGER, auto_ban INTEGER, test_model TEXT);
                INSERT INTO channels VALUES (3,1,'B',1,1,0,0,'https://b',0,0,'gpt','stable',10,1,'');
                INSERT INTO channels VALUES (2,1,'A',1,1,0,0,'https://a',0,0,'gpt-pro','stable',10,1,'');
                INSERT INTO channels VALUES (1,1,'P',1,1,0,0,'https://p',0,0,'gpt','profile',99,1,'');
                """
            )
            con.commit(); con.close()
            with patch.object(channel_monitor, "DB_PATH", db), patch.object(channel_monitor, "STATE_PATH", Path(td) / "state.json"):
                rows = channel_monitor.list_channels()
            self.assertEqual([row["id"] for row in rows], [2, 3, 1])
            self.assertEqual(rows[0]["combination_rank"], 1)


if __name__ == "__main__":
    unittest.main()
