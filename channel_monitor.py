"""Channel availability monitoring merged into API Hub.

Keeps the former /api/channel-monitor/* contract while sharing the Hub process.
The monitor state remains in the original channel-monitor data file so the
migration is reversible and existing history is preserved.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from functools import cmp_to_key
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query

DB_PATH = Path(os.getenv("NEWAPI_DB", "/home/youyang/projects/services/new-api/data/one-api.db"))
STATE_PATH = Path(os.getenv(
    "CHANNEL_MONITOR_STATE",
    "/home/youyang/projects/web-apps/main/channel-monitor/monitor-state.json",
))
LOG_PATH = Path(os.getenv(
    "PRIORITY_ADJUST_LOG",
    str(STATE_PATH.with_name("priority-adjust-log.json")),
))
ADMIN_TOKEN = os.getenv("CHANNEL_MONITOR_TOKEN", "")
DEFAULT_MONITOR_MODEL = "gpt-5.6-terra"
MAX_ACTION_LOG = 2000
router = APIRouter(prefix="/channel-monitor", tags=["channel-monitor"])


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject redirects so the New API PAT never crosses origins."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, msg, headers, fp)


def _open_newapi(request: urllib.request.Request, timeout: float):
    return urllib.request.build_opener(_NoRedirectHandler).open(request, timeout=timeout)


# Stable keys are persisted in data/settings.json; labels are presentation-only.
COMBINATION_OPTIONS: tuple[dict[str, str], ...] = (
    {"key": "pro_stable", "label": "Pro + 稳定"},
    {"key": "pro_stable_p20", "label": "Pro + 稳定 + P20"},
    {"key": "pro_stable_mixed", "label": "其他 Pro + 稳定混合"},
    {"key": "stable", "label": "稳定"},
    {"key": "plus", "label": "Plus"},
    {"key": "p20", "label": "P20"},
    {"key": "other", "label": "其他"},
)
DEFAULT_COMBINATION_ORDER: tuple[str, ...] = tuple(x["key"] for x in COMBINATION_OPTIONS)


def list_channel_group_names() -> list[str]:
    """Return every enabled pool currently present in New API abilities."""
    con = db_connect()
    try:
        rows = con.execute(
            'SELECT DISTINCT TRIM(a."group") '
            'FROM abilities a JOIN channels c ON c.id=a.channel_id '
            'WHERE a.enabled=1 AND c.status=1 AND LENGTH(TRIM(a."group"))>0 '
            'ORDER BY TRIM(a."group") COLLATE NOCASE'
        ).fetchall()
    finally:
        con.close()
    names = [str(row[0]) for row in rows if row and str(row[0]).strip()]
    preferred = [
        "gpt plus 号池",
        "gpt plus 稳定",
        "gpt plus/pro 混池",
        "gpt pro20x 号池",
    ]
    rank = {name: index for index, name in enumerate(preferred)}
    return sorted(names, key=lambda name: (rank.get(name, len(preferred)), name.casefold()))


def combination_options() -> list[dict[str, str]]:
    return [{"key": name, "label": name} for name in list_channel_group_names()]


def normalize_combination_order(value: Any, available: list[str] | None = None) -> list[str]:
    known = available if available is not None else list(DEFAULT_COMBINATION_ORDER)
    saved = value if isinstance(value, list) else []
    result = [x for x in saved if isinstance(x, str) and x in known]
    result = list(dict.fromkeys(result))
    return result + [x for x in known if x not in result]


def validate_combination_order(value: Any, available: list[str] | None = None) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(x, str) for x in value):
        raise ValueError("channel_combination_order 必须是字符串数组")
    ordered_known = available if available is not None else list(DEFAULT_COMBINATION_ORDER)
    known = set(ordered_known)
    if any(x not in known for x in value):
        raise ValueError("channel_combination_order 包含未知类别")
    if len(set(value)) != len(value):
        raise ValueError("channel_combination_order 不允许重复类别")
    return value + [x for x in ordered_known if x not in value]

def db_connect(read_only: bool = True) -> sqlite3.Connection:
    if read_only:
        return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    return sqlite3.connect(DB_PATH)


def load_state() -> dict[str, Any]:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def write_state(state: dict[str, Any]) -> None:
    with STATE_WRITE_LOCK:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary = STATE_PATH.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temporary.replace(STATE_PATH)


class _StateLock:
    """Synchronous lock for state writes; async callers use to_thread around writes."""
    def __init__(self):
        import threading
        self.lock = threading.Lock()

    def __enter__(self):
        self.lock.acquire()
        return self

    def __exit__(self, *_args):
        self.lock.release()


STATE_WRITE_LOCK = _StateLock()


def channel_row(row: tuple[Any, ...]) -> dict[str, Any]:
    cols = [
        "id", "type", "name", "status", "weight", "test_time", "response_time",
        "base_url", "balance", "balance_updated_time", "models", "group", "priority",
        "auto_ban", "test_model",
    ]
    item = dict(zip(cols, row))
    item["enabled"] = item["status"] == 1
    item["status_label"] = "启用" if item["enabled"] else "停用"
    item["models"] = [x.strip() for x in str(item.get("models") or "").split(",") if x.strip()]
    return item


def classify_combination(item: dict[str, Any]) -> str:
    text = " ".join([
        str(item.get("group", "")), str(item.get("name", "")),
        " ".join(item.get("models", []) or []), str(item.get("test_model", "")),
    ]).lower()
    stable = "稳定" in text or bool(re.search(r"\bstable\b", text))
    plus = bool(re.search(r"\bplus\b", text))
    p20 = bool(re.search(r"(?:\bp20\b|pro20x)", text))
    mixed = bool(re.search(r"(?:\bmix(?:ed)?\b|混合)", text))
    pro = bool(re.search(r"\bpro\b", text)) or bool(re.search(r"pro20x", text))
    if pro and stable and p20 and not plus:
        return "pro_stable_p20"
    if pro and stable and (plus or p20 or mixed):
        return "pro_stable_mixed"
    if pro and stable:
        return "pro_stable"
    if stable:
        return "stable"
    if plus:
        return "plus"
    if p20:
        return "p20"
    return "other"


def channel_combination_rank(item: dict[str, Any], order: list[str] | None = None) -> int:
    effective_order = order or list(DEFAULT_COMBINATION_ORDER)
    try:
        group_name = str(item.get("group") or "").strip()
        key = group_name if group_name in effective_order else classify_combination(item)
        return effective_order.index(key) + 1
    except ValueError:
        return len(effective_order) + 1


def get_combination_order() -> list[str]:
    try:
        from routes import _load_settings
        return normalize_combination_order(
            _load_settings().get("channel_combination_order"),
            list_channel_group_names(),
        )
    except Exception:
        return list(DEFAULT_COMBINATION_ORDER)


def _owner_metadata(channel_id: int) -> dict[str, Any]:
    try:
        from routes import get_channel_ownership, _account_names, SITE_CHANNEL_IDS_BY_ACCOUNT
        item = get_channel_ownership(channel_id)
        if item:
            owner_id = str(item.get("owner_account_id") or "")
            return {"owner_account_id": owner_id or None, "owner_account_name": _account_names().get(owner_id), "owner_source": "manual"}
        static = [str(account_id) for account_id, ids in SITE_CHANNEL_IDS_BY_ACCOUNT.items() if channel_id in ids]
        if len(static) == 1 and static[0] in _account_names():
            return {"owner_account_id": static[0], "owner_account_name": _account_names()[static[0]], "owner_source": "channel_id"}
    except Exception:
        pass
    return {"owner_account_id": None, "owner_account_name": None, "owner_source": None}


def history_since(history: list[dict[str, Any]], cutoff: datetime) -> list[dict[str, Any]]:
    result = []
    for item in history:
        try:
            if datetime.fromisoformat(item["at"]) >= cutoff:
                result.append(item)
        except (KeyError, TypeError, ValueError):
            continue
    return result


def availability(history: list[dict[str, Any]]) -> float | None:
    if not history:
        return None
    return round(sum(bool(item.get("ok")) for item in history) / len(history) * 100, 1)


# Routing consumes this Hub's existing monitor history. It never issues probes.
ROUTING_PRIORITY_RANGES = {
    "gpt plus 号池": (50, 59),
    "gpt plus 稳定": (40, 49),
    "gpt plus/pro 混池": (30, 39),
    "GPT 企业级线路": (15, 19),
    "gpt pro20x 号池": (20, 29),
    "grok havey": (11, 12),
}
TICK_SLOW_MS = 6000


def sample_tick(sample: dict[str, Any]) -> str:
    """Same colors as the Hub timeline: green <6s, yellow slow-ok, red fail."""
    if not sample.get("ok"):
        return "bad"
    if float(sample.get("latency_ms") or 0) >= TICK_SLOW_MS:
        return "degraded"
    return "ok"


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}


def routing_score_config() -> dict[str, Any]:
    return {
        "enabled": _env_bool("ROUTING_SCORE_ENABLED"),
        "auto_apply": _env_bool("ROUTING_SCORE_AUTO_APPLY"),
        "allow_write": _env_bool("ROUTING_SCORE_ALLOW_WRITE"),
        "min_samples": max(1, int(os.getenv("ROUTING_SCORE_MIN_SAMPLES", "2"))),
        "stability_samples": max(1, int(os.getenv("ROUTING_SCORE_STABILITY_SAMPLES", "5"))),
        "latest_max_age_minutes": max(1, int(os.getenv("ROUTING_SCORE_LATEST_MAX_AGE_MINUTES", "5"))),
        "previous_max_age_minutes": max(1, int(os.getenv("ROUTING_SCORE_PREVIOUS_MAX_AGE_MINUTES", "10"))),
        "speed_tie_points": max(0.0, float(os.getenv("ROUTING_SCORE_SPEED_TIE_POINTS", "5"))),
    }


def auto_toggle_config(state: dict[str, Any] | None = None) -> dict[str, Any]:
    saved = ((state or load_state()).get("_auto_toggle") or {})
    threshold = saved.get("fail_threshold", os.getenv("AUTO_TOGGLE_FAIL_THRESHOLD", "3"))
    return {
        "enabled": bool(saved["enabled"]) if "enabled" in saved else _env_bool("AUTO_TOGGLE_ENABLED", True),
        "fail_threshold": max(1, int(threshold)),
    }


def load_action_log() -> list[dict[str, Any]]:
    try:
        payload = json.loads(LOG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return payload if isinstance(payload, list) else []


def append_action_log(entries: list[dict[str, Any]]) -> None:
    if not entries:
        return
    now = datetime.now().astimezone().isoformat()
    with STATE_WRITE_LOCK:
        rows = load_action_log()
        for entry in entries:
            row = {"at": now, **entry}
            rows.append(row)
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary = LOG_PATH.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(rows[-MAX_ACTION_LOG:], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temporary.replace(LOG_PATH)


def _routing_metrics(saved: dict[str, Any], now: datetime, config: dict[str, Any]) -> dict[str, Any]:
    parsed_samples = []
    for item in saved.get("history", []):
        try:
            parsed_samples.append((datetime.fromisoformat(item["at"]), item))
        except (KeyError, TypeError, ValueError):
            continue
    parsed_samples.sort(key=lambda entry: entry[0], reverse=True)
    latest = parsed_samples[: config["min_samples"]]
    samples = [item for _, item in latest]
    latest_at = latest[0][0] if latest else None
    previous_at = latest[1][0] if len(latest) > 1 else None
    latest_fresh = bool(latest_at and latest_at >= now - timedelta(minutes=config["latest_max_age_minutes"]))
    previous_fresh = bool(previous_at and previous_at >= now - timedelta(minutes=config["previous_max_age_minutes"]))
    successful = [sample for sample in samples if sample.get("ok")]
    latencies = [float(sample.get("latency_ms") or 0) for sample in successful if float(sample.get("latency_ms") or 0) > 0]
    stability_window = [item for _, item in parsed_samples[: config["stability_samples"]]]
    ticks = [sample_tick(sample) for sample in stability_window]
    green_count = sum(1 for tick in ticks if tick == "ok")
    yellow_count = sum(1 for tick in ticks if tick == "degraded")
    red_count = sum(1 for tick in ticks if tick == "bad")
    stability = round((green_count + yellow_count * 0.5) * 100 / len(stability_window), 1) if stability_window else 0.0
    return {
        "sample_count": len(samples),
        "success_count": len(successful),
        "availability": round(len(successful) * 100 / len(samples), 1) if samples else None,
        "average_latency_ms": round(sum(latencies) / len(latencies), 1) if latencies else None,
        "stability": stability,
        "stability_sample_count": len(stability_window),
        "green_count": green_count,
        "yellow_count": yellow_count,
        "red_count": red_count,
        "latest_sample_at": latest_at.isoformat() if latest_at else None,
        "previous_sample_at": previous_at.isoformat() if previous_at else None,
        "latest_fresh": latest_fresh,
        "previous_fresh": previous_fresh,
        "eligible": len(samples) >= config["min_samples"] and latest_fresh and previous_fresh,
    }


def calculate_routing_scores(now: datetime | None = None) -> list[dict[str, Any]]:
    """Score monitored, routable channels from the existing rolling history."""
    config = routing_score_config()
    now = now or datetime.now().astimezone()
    candidates = {}
    for channel in list_channels():
        if not (channel["channel_enabled"] and channel["monitor_enabled"] and channel["category_enabled"]):
            continue
        candidates[int(channel["id"])] = channel
    if not candidates:
        return []

    con = db_connect()
    try:
        rows = con.execute(
            'SELECT DISTINCT a."group", a.channel_id, c.name, c.priority '
            'FROM abilities a JOIN channels c ON c.id=a.channel_id '
            'WHERE a.enabled=1 AND c.status=1'
        ).fetchall()
    finally:
        con.close()

    by_group = {}
    for group, channel_id, name, priority in rows:
        channel_id = int(channel_id)
        if channel_id not in candidates or group not in ROUTING_PRIORITY_RANGES:
            continue
        entry = {
            "group": str(group), "channel_id": channel_id, "channel_name": str(name or channel_id),
            "current_priority": int(priority or 0), **_routing_metrics(candidates[channel_id], now, config),
        }
        by_group.setdefault(str(group), []).append(entry)

    result = []
    for group, entries in by_group.items():
        eligible = [entry for entry in entries if entry["eligible"]]
        successful = [entry for entry in eligible if entry["average_latency_ms"] is not None]
        fastest = min((entry["average_latency_ms"] for entry in successful), default=None)
        for entry in entries:
            if not entry["eligible"]:
                if entry["sample_count"] < config["min_samples"]:
                    reason = f"样本不足（{entry['sample_count']}/{config['min_samples']}）"
                elif not entry["latest_fresh"]:
                    reason = f"最新样本超过{config['latest_max_age_minutes']}分钟有效期"
                else:
                    reason = f"第二条样本超过{config['previous_max_age_minutes']}分钟有效期"
                entry.update(score=None, target_priority=None, reason=reason)
                continue
            latency = entry["average_latency_ms"]
            latency_score = 0.0 if not fastest or not latency else min(100.0, fastest * 100 / latency)
            entry["speed_score"] = round(latency_score, 1)
            entry["composite_score"] = round(latency_score * .7 + entry["stability"] * .3, 1)
            entry["score"] = round(float(entry["availability"] or 0) * .6 + entry["composite_score"] * .4, 1)
        # Availability and speed from last 2; stability from last 5 green/yellow/red ticks.
        def compare(left: dict[str, Any], right: dict[str, Any]) -> int:
            if left["score"] != right["score"]:
                return -1 if left["score"] > right["score"] else 1
            if left["availability"] != right["availability"]:
                return -1 if left["availability"] > right["availability"] else 1
            if abs(left["speed_score"] - right["speed_score"]) > config["speed_tie_points"]:
                return -1 if left["speed_score"] > right["speed_score"] else 1
            if left["stability"] != right["stability"]:
                return -1 if left["stability"] > right["stability"] else 1
            if left["current_priority"] != right["current_priority"]:
                return -1 if left["current_priority"] > right["current_priority"] else 1
            return -1 if left["channel_id"] < right["channel_id"] else (1 if left["channel_id"] > right["channel_id"] else 0)

        ranked = sorted(eligible, key=cmp_to_key(compare))
        low, high = ROUTING_PRIORITY_RANGES[group]
        degraded = ranked and max(entry["availability"] or 0 for entry in ranked) < 50
        for rank, entry in enumerate(ranked, start=1):
            entry["target_priority"] = low if degraded else max(low, high - rank + 1)
            entry["rank"] = rank
            entry["reason"] = "成功率/速度用最近2条，稳定性用最近5条色块：可用性60% + 相对延迟30% + 稳定性10%"
            if degraded:
                entry["reason"] += "；号池整体异常，统一压低"
        result.extend(entries)

    by_channel = {}
    for entry in result:
        if entry.get("target_priority") is not None:
            by_channel.setdefault(entry["channel_id"], set()).add(entry["target_priority"])
    for entry in result:
        if len(by_channel.get(entry["channel_id"], set())) > 1:
            entry.update(target_priority=None, conflict=True, reason="跨分组目标冲突，跳过自动写入")
    return result


def apply_routing_scores() -> dict[str, Any]:
    config = routing_score_config()
    scores = calculate_routing_scores()
    by_id = {}
    for entry in scores:
        by_id.setdefault(entry["channel_id"], entry)
    targets = {entry["channel_id"]: entry["target_priority"] for entry in scores if entry.get("target_priority") is not None}
    updates = [(channel_id, target) for channel_id, target in targets.items() if by_id[channel_id]["current_priority"] != target]
    if not config["allow_write"]:
        return {"ok": False, "applied": False, "updated": 0, "scores": scores, "changes": []}
    changes = []
    con = db_connect(False)
    try:
        for channel_id, target in updates:
            current = int(by_id[channel_id]["current_priority"])
            con.execute("UPDATE channels SET priority=? WHERE id=?", (target, channel_id))
            con.execute("UPDATE abilities SET priority=? WHERE channel_id=? AND enabled=1", (target, channel_id))
            changes.append({
                "kind": "priority",
                "channel_id": channel_id,
                "channel_name": by_id[channel_id].get("channel_name"),
                "group": by_id[channel_id].get("group"),
                "from_priority": current,
                "to_priority": target,
                "score": by_id[channel_id].get("score"),
                "reason": by_id[channel_id].get("reason"),
            })
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    append_action_log(changes)
    return {"ok": True, "applied": True, "updated": len(updates), "scores": scores, "changes": changes}


def set_channel_enabled(channel_id: int, enabled: bool) -> bool:
    """Keep the channel switch and model abilities in sync atomically."""
    con = db_connect(False)
    try:
        con.execute("BEGIN IMMEDIATE")
        status = 1 if enabled else 2
        ability_enabled = 1 if enabled else 0
        cur = con.execute("UPDATE channels SET status=? WHERE id=?", (status, channel_id))
        if cur.rowcount != 1:
            con.rollback()
            return False
        con.execute(
            "UPDATE abilities SET enabled=? WHERE channel_id=?",
            (ability_enabled, channel_id),
        )
        con.commit()
        return True
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def pool_lowest_priority(channel_id: int, fallback_group: str | None = None) -> int | None:
    """Return the lowest slot of this channel's routing pool, or None if it is not in a scored pool."""
    con = db_connect()
    try:
        rows = con.execute(
            'SELECT DISTINCT a."group" FROM abilities a WHERE a.channel_id=? AND a.enabled=1',
            (channel_id,),
        ).fetchall()
    finally:
        con.close()
    lows: set[int] = set()
    for (group,) in rows:
        if group in ROUTING_PRIORITY_RANGES:
            lows.add(ROUTING_PRIORITY_RANGES[group][0])
    if not lows and fallback_group in ROUTING_PRIORITY_RANGES:
        lows.add(ROUTING_PRIORITY_RANGES[fallback_group][0])
    if len(lows) == 1:
        return next(iter(lows))
    return None


def set_channel_priority(channel_id: int, priority: int) -> int | None:
    con = db_connect(False)
    try:
        row = con.execute("SELECT priority FROM channels WHERE id=?", (channel_id,)).fetchone()
        if not row:
            return None
        current = int(row[0] or 0)
        con.execute("UPDATE channels SET priority=? WHERE id=?", (priority, channel_id))
        con.execute("UPDATE abilities SET priority=? WHERE channel_id=? AND enabled=1", (priority, channel_id))
        con.commit()
        return current
    finally:
        con.close()


def drop_disabled_channel_to_pool_floor(
    channel_id: int,
    fallback_group: str | None = None,
    channel_name: str | None = None,
    reason: str = "渠道已禁用，优先级调到分组最低",
) -> dict[str, Any] | None:
    """If a scored-pool channel is disabled above its floor, drop it to the floor."""
    con = db_connect()
    try:
        row = con.execute(
            'SELECT name, status, priority, "group" FROM channels WHERE id=?',
            (channel_id,),
        ).fetchone()
    finally:
        con.close()
    if not row:
        return None
    name, status, current_priority, group = row
    if int(status or 0) == 1:
        return None
    lowest = pool_lowest_priority(channel_id, fallback_group or group)
    if lowest is None:
        return None
    current_priority = int(current_priority or 0)
    if current_priority <= lowest:
        return None
    previous = set_channel_priority(channel_id, lowest)
    if previous is None:
        return None
    action = {
        "kind": "priority",
        "channel_id": channel_id,
        "channel_name": channel_name or name,
        "group": fallback_group or group,
        "from_priority": previous,
        "to_priority": lowest,
        "reason": reason,
    }
    append_action_log([action])
    return action


def drop_disabled_scored_channels_to_floor() -> list[dict[str, Any]]:
    """Read New API channel status directly and drop disabled scored-pool channels to their floor."""
    con = db_connect()
    try:
        rows = con.execute(
            'SELECT c.id, c.name, c.status, c.priority, c."group", '
            'GROUP_CONCAT(DISTINCT a."group") '
            'FROM channels c LEFT JOIN abilities a ON a.channel_id=c.id AND a.enabled=1 '
            'GROUP BY c.id'
        ).fetchall()
    finally:
        con.close()
    dropped: list[dict[str, Any]] = []
    for channel_id, name, status, priority, group, agroups in rows:
        if int(status or 0) == 1:
            continue
        groups = [g for g in str(agroups or group or "").split(",") if g]
        lows = {ROUTING_PRIORITY_RANGES[g][0] for g in groups if g in ROUTING_PRIORITY_RANGES}
        if len(lows) != 1:
            continue
        lowest = next(iter(lows))
        if int(priority or 0) <= lowest:
            continue
        action = drop_disabled_channel_to_pool_floor(
            int(channel_id),
            fallback_group=group,
            channel_name=name,
            reason="New API 渠道已禁用，优先级调到分组最低",
        )
        if action:
            dropped.append(action)
    return dropped


def scored_pool_names(channel_id: int, fallback_group: str | None = None) -> list[str]:
    con = db_connect()
    try:
        rows = con.execute(
            'SELECT DISTINCT a."group" FROM abilities a WHERE a.channel_id=? AND a.enabled=1',
            (channel_id,),
        ).fetchall()
    finally:
        con.close()
    groups = [group for (group,) in rows if group in ROUTING_PRIORITY_RANGES]
    if not groups and fallback_group in ROUTING_PRIORITY_RANGES:
        groups = [fallback_group]
    return groups


def enabled_channel_count_in_pool(group: str) -> int:
    con = db_connect()
    try:
        row = con.execute(
            'SELECT COUNT(DISTINCT c.id) FROM channels c '
            'JOIN abilities a ON a.channel_id=c.id AND a.enabled=1 '
            'WHERE c.status=1 AND a."group"=?',
            (group,),
        ).fetchone()
    finally:
        con.close()
    return int(row[0] if row else 0)


def is_last_enabled_in_any_scored_pool(channel_id: int, fallback_group: str | None = None) -> bool:
    """True if closing this channel would empty any of its routing pools."""
    for group in scored_pool_names(channel_id, fallback_group):
        if enabled_channel_count_in_pool(group) <= 1:
            return True
    return False


def apply_auto_toggle(channel: dict[str, Any], result: dict[str, Any]) -> dict[str, Any] | None:
    """Enable/disable the New API channel from monitor results. Monitoring itself never stops."""
    config = auto_toggle_config()
    if not config["enabled"]:
        return None
    if channel.get("auto_toggle_exempt"):
        return None
    channel_id = int(channel["id"])
    currently_enabled = bool(channel.get("channel_enabled"))
    failures = int(result.get("consecutive_failures") or 0)
    last_ok = bool(result.get("last_ok"))
    action = None
    if currently_enabled and failures >= config["fail_threshold"]:
        if is_last_enabled_in_any_scored_pool(channel_id, channel.get("group")):
            return None
        if set_channel_enabled(channel_id, False):
            dropped = drop_disabled_channel_to_pool_floor(
                channel_id,
                fallback_group=channel.get("group"),
                channel_name=channel.get("name"),
                reason=f"连续失败 {failures} 次，关闭 New API 渠道，优先级调到分组最低",
            )
            lowest = None if dropped is None else dropped.get("to_priority")
            action = {
                "kind": "disable",
                "channel_id": channel_id,
                "channel_name": channel.get("name"),
                "group": channel.get("group"),
                "from_enabled": True,
                "to_enabled": False,
                "consecutive_failures": failures,
                "from_priority": None if dropped is None else dropped.get("from_priority"),
                "to_priority": lowest,
                "reason": (
                    f"连续失败 {failures} 次，关闭 New API 渠道"
                    + (f"，优先级调到分组最低 {lowest}" if lowest is not None else "")
                ),
            }
    elif (not currently_enabled) and last_ok:
        if set_channel_enabled(channel_id, True):
            action = {
                "kind": "enable",
                "channel_id": channel_id,
                "channel_name": channel.get("name"),
                "group": channel.get("group"),
                "from_enabled": False,
                "to_enabled": True,
                "consecutive_failures": 0,
                "reason": "检测到可用，打开 New API 渠道",
            }
    if action:
        append_action_log([action])
    return action


@router.get("/routing-score")
def routing_score() -> dict[str, Any]:
    return {"ok": True, "config": routing_score_config(), "scores": calculate_routing_scores()}


@router.get("/priority-log")
def priority_log(limit: int = Query(default=200, ge=1, le=1000)) -> dict[str, Any]:
    rows = load_action_log()
    return {
        "ok": True,
        "config": {**routing_score_config(), "auto_toggle": auto_toggle_config()},
        "logs": list(reversed(rows[-limit:])),
        "total": len(rows),
    }


@router.post("/auto-toggle")
def save_auto_toggle(payload: dict[str, Any], x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    state = load_state()
    item = state.setdefault("_auto_toggle", {})
    if "enabled" in payload:
        item["enabled"] = bool(payload.get("enabled"))
    if "fail_threshold" in payload and payload.get("fail_threshold") not in (None, ""):
        item["fail_threshold"] = max(1, int(payload.get("fail_threshold") or 3))
    write_state(state)
    return {"ok": True, **auto_toggle_config(state)}


@router.post("/channels/{channel_id}/auto-toggle-exempt")
def save_auto_toggle_exempt(channel_id: int, payload: dict[str, Any], x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    exempt = bool(payload.get("exempt"))
    save_monitor_settings(channel_id, auto_toggle_exempt=exempt)
    return {"ok": True, "id": channel_id, "exempt": exempt}


def get_monitor_global_enabled(state: dict[str, Any] | None = None) -> bool:
    return bool((state or load_state()).get("_global", {}).get("enabled", True))


def get_category_enabled(category: str, state: dict[str, Any] | None = None) -> bool:
    return bool((state or load_state()).get("_categories", {}).get(category, True))


def classify_channel(item: dict[str, Any]) -> str:
    text = " ".join([
        str(item.get("name", "")),
        str(item.get("base_url", "")),
        str(item.get("test_model", "")),
        " ".join(item.get("models", [])),
    ]).lower()
    if any(x in text for x in ("claude", "anthropic")):
        return "claude"
    if "grok" in text:
        return "grok"
    if any(x in text for x in ("gpt", "openai", "chatgpt", "o1-", "o3-", "o4-", "gpt-")):
        return "gpt"
    return "other"


def save_monitor_settings(
    channel_id: int,
    enabled: bool | None = None,
    model: str | None = None,
    auto_toggle_exempt: bool | None = None,
) -> None:
    state = load_state()
    item = state.setdefault(str(channel_id), {})
    if enabled is not None:
        item["monitor_enabled"] = bool(enabled)
    if model is not None:
        item["monitor_model"] = str(model).strip()
    if auto_toggle_exempt is not None:
        item["auto_toggle_exempt"] = bool(auto_toggle_exempt)
    write_state(state)


def set_state_value(path: str, value: Any) -> None:
    state = load_state()
    if path == "global":
        state.setdefault("_global", {})["enabled"] = bool(value)
    elif path.startswith("category:"):
        state.setdefault("_categories", {})[path.split(":", 1)[1]] = bool(value)
    write_state(state)


def list_channels() -> list[dict[str, Any]]:
    con = db_connect()
    try:
        rows = con.execute(
            'SELECT id,type,name,status,weight,test_time,response_time,base_url,balance,'
            'balance_updated_time,models,"group",priority,auto_ban,test_model '
            'FROM channels ORDER BY priority DESC,id'
        ).fetchall()
    finally:
        con.close()
    state = load_state()
    now = datetime.now().astimezone()
    result = []
    combination_order = get_combination_order()
    for row in rows:
        item = channel_row(row)
        saved = state.get(str(item["id"]), {})
        item.update(saved)
        history = saved.get("history", [])
        item["availability_24h"] = availability(history_since(history, now - timedelta(hours=24)))
        item["availability_7d"] = availability(history_since(history, now - timedelta(days=7)))
        item["channel_enabled"] = item["status"] == 1
        item["monitor_enabled"] = saved.get("monitor_enabled", True)
        saved_model = saved.get("monitor_model")
        item["monitor_model"] = saved_model or (
            DEFAULT_MONITOR_MODEL if DEFAULT_MONITOR_MODEL in item["models"] else (item["models"] or [""])[0]
        )
        item["monitor_model_saved"] = bool(saved_model)
        item["monitor_global_enabled"] = get_monitor_global_enabled(state)
        item["auto_toggle_exempt"] = bool(saved.get("auto_toggle_exempt", False))
        item["category"] = classify_channel(item)
        item["category_enabled"] = get_category_enabled(item["category"], state)
        item["combination"] = classify_combination(item)
        item["combination_rank"] = channel_combination_rank(item, combination_order)
        item.update(_owner_metadata(int(item["id"])))
        result.append(item)
    result.sort(key=lambda x: (
        int(x.get("combination_rank", len(combination_order) + 1)),
        -int(x.get("priority") or 0),
        str(x.get("name") or ""),
        int(x.get("id") or 0),
    ))
    return result


def run_check(channel: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    model = channel.get("monitor_model") or DEFAULT_MONITOR_MODEL
    base = os.getenv("NEWAPI_BASE_URL", "").strip().rstrip("/")
    admin_token = os.getenv("NEWAPI_ADMIN_TOKEN", "").strip()
    missing = [
        name for name, value in (
            ("NEWAPI_BASE_URL", base),
            ("NEWAPI_ADMIN_TOKEN", admin_token),
        ) if not value
    ]
    if missing:
        return {
            "last_ok": False, "last_http": None, "last_latency_ms": 0,
            "last_message": f"New API 探活配置缺失: {', '.join(missing)}", "last_model": model,
            "last_checked_at": datetime.now().astimezone().isoformat(),
        }

    query = urllib.parse.urlencode({"model": model})
    url = f"{base}/api/channel/test/{int(channel['id'])}?{query}"
    headers = {
        "Authorization": "Bearer " + admin_token,
        "Accept": "application/json",
        "User-Agent": "new-api-channel-monitor/1.0",
    }
    ok = False
    code = None
    message = ""
    latency_ms = None
    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with _open_newapi(req, timeout=30) as response:
            body = response.read(32768)
            code = response.status
        data = json.loads(body)
        if not isinstance(data, dict):
            raise TypeError("New API 返回格式不是对象")
        api_time = data.get("time")
        if isinstance(api_time, (int, float)) and not isinstance(api_time, bool) and math.isfinite(api_time) and api_time >= 0:
            latency_ms = round(api_time * 1000)
        ok = 200 <= code < 300 and data.get("success") is True
        message = str(data.get("message") or ("New API 渠道测试通过" if ok else "New API 渠道测试失败"))
    except urllib.error.HTTPError as exc:
        code = exc.code
        message = f"New API HTTP {exc.code}: {exc.reason}"
    except Exception as exc:
        code = getattr(exc, "code", code)
        message = f"New API 渠道测试请求失败: {exc}"
    if admin_token:
        message = message.replace(admin_token, "[REDACTED]")
    return {
        "last_ok": ok, "last_http": code,
        "last_latency_ms": latency_ms if latency_ms is not None else round((time.perf_counter() - started) * 1000),
        "last_message": message[:180], "last_model": model,
        "last_checked_at": datetime.now().astimezone().isoformat(),
    }


def persist_check(channel_id: int, result: dict[str, Any], channel: dict[str, Any] | None = None) -> dict[str, Any] | None:
    state = load_state()
    old = state.get(str(channel_id), {})
    history = old.get("history", [])
    history.append({"ok": result["last_ok"], "latency_ms": result["last_latency_ms"], "at": result["last_checked_at"]})
    now = datetime.now().astimezone()
    history = history_since(history, now - timedelta(days=7))[-10080:]
    result["history"] = history
    result["consecutive_failures"] = (old.get("consecutive_failures", 0) + 1) if not result["last_ok"] else 0
    result["availability_24h"] = availability(history_since(history, now - timedelta(hours=24)))
    result["availability_7d"] = availability(history)
    state[str(channel_id)] = {**old, **result}
    write_state(state)
    source = channel or old
    source = {**source, "id": channel_id, "channel_enabled": source.get("channel_enabled", source.get("status") == 1)}
    return apply_auto_toggle(source, result)


def check_token(_provided: str | None = None) -> None:
    """Merged HUB runs locally behind its existing access boundary; no second token prompt."""
    return None


def token_value(x_channel_monitor_token: str | None, authorization: str | None) -> str:
    if authorization and authorization.startswith("Bearer "):
        return authorization[7:]
    return x_channel_monitor_token or ""


@router.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "db": str(DB_PATH), "time": datetime.now().isoformat()}


@router.get("/channels")
def channels(x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    return {"ok": True, "channels": list_channels()}


@router.get("/test/{channel_id}")
def test_channel(channel_id: int, x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    channel = next((x for x in list_channels() if x["id"] == channel_id), None)
    if not channel:
        raise HTTPException(status_code=404, detail="渠道不存在")
    result = run_check(channel)
    persist_check(channel_id, result, channel)
    return {"ok": result["last_ok"], **result}


@router.post("/global-toggle")
async def global_toggle(payload: dict[str, Any], x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    set_state_value("global", payload.get("enabled"))
    return {"ok": True, "enabled": get_monitor_global_enabled()}


@router.post("/category-toggle")
async def category_toggle(payload: dict[str, Any], x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    category = str(payload.get("category", "other"))
    set_state_value("category:" + category, payload.get("enabled"))
    return {"ok": True, "category": category, "enabled": get_category_enabled(category)}


@router.post("/channels/{channel_id}/monitor-toggle")
async def monitor_toggle(channel_id: int, payload: dict[str, Any], x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    save_monitor_settings(channel_id, enabled=payload.get("enabled"))
    return {"ok": True}


@router.post("/channels/{channel_id}/monitor-model")
async def monitor_model(channel_id: int, payload: dict[str, Any], x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    model = str(payload.get("model", "")).strip()
    if not model:
        raise HTTPException(status_code=400, detail="测试模型不能为空")
    save_monitor_settings(channel_id, model=model)
    return {"ok": True, "model": model}


def toggle_channel_enabled(channel_id: int, enabled: bool) -> dict[str, Any]:
    if not set_channel_enabled(channel_id, enabled):
        raise HTTPException(status_code=404, detail="渠道不存在")
    dropped = None
    if not enabled:
        dropped = drop_disabled_channel_to_pool_floor(
            channel_id,
            reason="手动禁用，优先级调到分组最低",
        )
    return {"ok": True, "id": channel_id, "enabled": enabled, "priority_dropped": dropped}


@router.post("/channels/{channel_id}/toggle")
def production_toggle(channel_id: int, payload: dict[str, Any], x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    return toggle_channel_enabled(channel_id, bool(payload.get("enabled")))


@router.post("/channels")
def add_channel(payload: dict[str, Any], x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    name = str(payload.get("name", "")).strip()
    base = str(payload.get("base_url", "")).strip()
    key = str(payload.get("key", "")).strip()
    if not name or not base or not key or not base.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="名称、上游地址和 Key 必填")
    models = ",".join(str(payload.get("models", "")).split(","))
    con = db_connect(False)
    try:
        cur = con.execute(
            'INSERT INTO channels (type,key,name,status,weight,base_url,models,"group",priority,auto_ban,test_model) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (1, key, name, 1, 1, base, models, str(payload.get("group", "default")), int(payload.get("priority", 0)), 1, str(payload.get("test_model", ""))),
        )
        con.commit()
        return {"ok": True, "id": cur.lastrowid}
    finally:
        con.close()


async def monitor_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            if get_monitor_global_enabled():
                for channel in list_channels():
                    if channel["monitor_enabled"] and channel["category_enabled"]:
                        result = await asyncio.to_thread(run_check, channel)
                        persist_check(channel["id"], result, channel)
                config = routing_score_config()
                if config["enabled"] and config["auto_apply"]:
                    applied = apply_routing_scores()
                    print(f"[routing-score] updated={applied['updated']}")
        except Exception:
            # One bad upstream must not stop the merged Hub monitor.
            pass
        try:
            dropped = drop_disabled_scored_channels_to_floor()
            if dropped:
                print(f"[routing-score] disabled-floor={len(dropped)}")
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=60)
        except asyncio.TimeoutError:
            continue


async def start_monitor_task() -> tuple[asyncio.Event, asyncio.Task[None]]:
    stop_event = asyncio.Event()
    return stop_event, asyncio.create_task(monitor_loop(stop_event), name="channel-monitor")
