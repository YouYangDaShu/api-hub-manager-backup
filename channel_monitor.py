"""Channel availability monitoring merged into API Hub.

Keeps the former /api/channel-monitor/* contract while sharing the Hub process.
The monitor state remains in the original channel-monitor data file so the
migration is reversible and existing history is preserved.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import sqlite3
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Header, HTTPException

DB_PATH = Path(os.getenv("NEWAPI_DB", "/home/youyang/projects/services/new-api/data/one-api.db"))
STATE_PATH = Path(os.getenv(
    "CHANNEL_MONITOR_STATE",
    "/home/youyang/projects/web-apps/main/channel-monitor/monitor-state.json",
))
ADMIN_TOKEN = os.getenv("CHANNEL_MONITOR_TOKEN", "")
DEFAULT_MONITOR_MODEL = "gpt-5.6-terra"
router = APIRouter(prefix="/channel-monitor", tags=["channel-monitor"])

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
COMBINATION_LABELS = {x["key"]: x["label"] for x in COMBINATION_OPTIONS}


def normalize_combination_order(value: Any) -> list[str]:
    """Return the default order unless a stored value is a complete valid order."""
    if not isinstance(value, list) or len(value) != len(DEFAULT_COMBINATION_ORDER):
        return list(DEFAULT_COMBINATION_ORDER)
    if any(not isinstance(x, str) for x in value) or len(set(value)) != len(value):
        return list(DEFAULT_COMBINATION_ORDER)
    if set(value) != set(DEFAULT_COMBINATION_ORDER):
        return list(DEFAULT_COMBINATION_ORDER)
    return list(value)


def validate_combination_order(value: Any) -> list[str]:
    """Validate a submitted order and append omitted known categories."""
    if not isinstance(value, list) or any(not isinstance(x, str) for x in value):
        raise ValueError("channel_combination_order 必须是字符串数组")
    known = set(DEFAULT_COMBINATION_ORDER)
    if any(x not in known for x in value):
        raise ValueError("channel_combination_order 包含未知类别")
    if len(set(value)) != len(value):
        raise ValueError("channel_combination_order 不允许重复类别")
    return value + [x for x in DEFAULT_COMBINATION_ORDER if x not in value]

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
    """Return a stable combination key from channel metadata."""
    text = " ".join([
        str(item.get("group", "")), str(item.get("name", "")),
        " ".join(item.get("models", []) or []), str(item.get("test_model", "")),
    ]).lower()
    stable = "稳定" in text or bool(re.search(r"\bstable\b", text))
    plus = bool(re.search(r"\bplus\b", text))
    p20 = bool(re.search(r"(?:\bp20\b|pro20x)", text))
    mixed = bool(re.search(r"(?:\bmix(?:ed)?\b|混合)", text))
    # \bpro\b recognizes GPT-PRO and Chinese-separated "pro" but not profile.
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
    """Return the one-based position used by API and UI sorting."""
    effective_order = order or list(DEFAULT_COMBINATION_ORDER)
    key = classify_combination(item)
    try:
        return effective_order.index(key) + 1
    except ValueError:
        return len(effective_order) + 1


def get_combination_order() -> list[str]:
    """Read the persisted order without importing routes at module load time."""
    try:
        from routes import _load_settings
        return normalize_combination_order(_load_settings().get("channel_combination_order"))
    except Exception:
        return list(DEFAULT_COMBINATION_ORDER)


def _safe_owner_metadata(channel_id: int) -> dict[str, Any]:
    try:
        from routes import get_channel_ownership, _account_names, SITE_CHANNEL_IDS_BY_ACCOUNT
        item = get_channel_ownership(channel_id)
        if not item:
            static = [str(account_id) for account_id, ids in SITE_CHANNEL_IDS_BY_ACCOUNT.items() if channel_id in ids]
            if len(static) == 1:
                names = _account_names()
                if static[0] in names:
                    return {"owner_account_id": static[0], "owner_account_name": names.get(static[0]), "owner_source": "channel_id", "source": "channel_id"}
            return {"owner_account_id": None, "owner_account_name": None, "owner_source": None, "source": None}
        owner_id = str(item.get("owner_account_id") or "")
        return {
            "owner_account_id": owner_id or None,
            "owner_account_name": _account_names().get(owner_id) if owner_id else None,
            "owner_source": item.get("source") or "manual",
            "source": item.get("source") or "manual",
            "owner_updated_at": item.get("updated_at") or None,
        }
    except Exception:
        return {"owner_account_id": None, "owner_account_name": None, "owner_source": None, "source": None}


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


def load_key(channel_id: int) -> str:
    con = db_connect()
    try:
        row = con.execute("SELECT key FROM channels WHERE id=?", (channel_id,)).fetchone()
    finally:
        con.close()
    raw = row[0] if row else ""
    return next((line.strip() for line in str(raw).splitlines() if line.strip()), "")


def fetch_upstream_models(channel_id: int) -> list[str]:
    con = db_connect()
    try:
        row = con.execute("SELECT base_url FROM channels WHERE id=?", (channel_id,)).fetchone()
    finally:
        con.close()
    if not row:
        return []
    key = load_key(channel_id)
    req = urllib.request.Request(
        (row[0] or "").rstrip("/") + "/v1/models",
        headers={"Authorization": "Bearer " + key, "User-Agent": "new-api-channel-monitor/1.0"},
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        data = json.loads(response.read(1024 * 1024))
    return [str(x.get("id")) for x in data.get("data", []) if isinstance(x, dict) and x.get("id")]


def save_monitor_settings(channel_id: int, enabled: bool | None = None, model: str | None = None) -> None:
    state = load_state()
    item = state.setdefault(str(channel_id), {})
    if enabled is not None:
        item["monitor_enabled"] = bool(enabled)
    if model is not None:
        item["monitor_model"] = str(model).strip()
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
    combination_order = get_combination_order()
    result = []
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
        item["category"] = classify_channel(item)
        item["category_enabled"] = get_category_enabled(item["category"], state)
        item["combination_key"] = classify_combination(item)
        item["combination_rank"] = channel_combination_rank(item, combination_order)
        item.update(_safe_owner_metadata(int(item["id"])))
        result.append(item)
    return sorted(result, key=lambda x: (
        int(x.get("combination_rank", len(combination_order) + 1)),
        -int(x.get("priority") or 0),
        str(x.get("name") or "").casefold(),
        int(x.get("id") or 0),
    ))


def run_check(channel: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    base = (channel.get("base_url") or "").rstrip("/")
    model = channel.get("monitor_model") or DEFAULT_MONITOR_MODEL
    key = load_key(int(channel["id"]))
    if not channel.get("monitor_model_saved"):
        try:
            upstream_models = fetch_upstream_models(int(channel["id"]))
            if model not in upstream_models and upstream_models:
                model = upstream_models[0]
                save_monitor_settings(int(channel["id"]), model=model)
        except Exception as exc:  # upstream errors are rendered as monitor failures
            return {
                "last_ok": False, "last_http": getattr(exc, "code", None), "last_latency_ms": 0,
                "last_message": f"首次拉取模型失败: {str(exc)[:140]}", "last_model": model,
                "last_checked_at": datetime.now().astimezone().isoformat(),
            }
    a, b = secrets.randbelow(40) + 10, secrets.randbelow(40) + 10
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": f"只回复数字：{a}+{b}="}],
        "max_tokens": 8, "temperature": 0, "stream": False,
    }
    headers = {"User-Agent": "new-api-channel-monitor/1.0", "Content-Type": "application/json"}
    if key:
        headers["Authorization"] = "Bearer " + key
    ok = False
    code = None
    message = ""
    try:
        req = urllib.request.Request(base + "/v1/chat/completions", data=json.dumps(payload).encode(), headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=30) as response:
            body = response.read(32768)
            code = response.status
        data = json.loads(body)
        text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        numbers = re.findall(r"\d+", str(text))
        ok = 200 <= code < 300 and str(a + b) in numbers
        message = "challenge通过" if ok else f"challenge失败，返回: {str(text)[:100]}"
    except Exception as exc:
        code = getattr(exc, "code", None)
        message = str(exc)[:180]
    return {
        "last_ok": ok, "last_http": code,
        "last_latency_ms": round((time.perf_counter() - started) * 1000),
        "last_message": message, "last_model": model,
        "last_checked_at": datetime.now().astimezone().isoformat(),
    }


def persist_check(channel_id: int, result: dict[str, Any]) -> None:
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


@router.get("/owners")
def owners(x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    try:
        from routes import _account_names
        return {"ok": True, "owners": [{"id": key, "name": name} for key, name in _account_names().items()]}
    except Exception:
        return {"ok": True, "owners": []}


@router.get("/test/{channel_id}")
def test_channel(channel_id: int, x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    channel = next((x for x in list_channels() if x["id"] == channel_id), None)
    if not channel:
        raise HTTPException(status_code=404, detail="渠道不存在")
    result = run_check(channel)
    persist_check(channel_id, result)
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


@router.put("/channels/{channel_id}/owner")
def set_channel_owner(channel_id: int, payload: dict[str, Any], x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    from routes import clear_channel_ownership, set_channel_ownership
    owner = payload.get("owner_account_id")
    if owner in (None, ""):
        clear_channel_ownership(channel_id)
        return {"ok": True, "owner_account_id": None}
    return {"ok": True, **set_channel_ownership(channel_id, str(owner), "manual")}


@router.delete("/channels/{channel_id}/owner")
def clear_channel_owner(channel_id: int, x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    from routes import clear_channel_ownership
    clear_channel_ownership(channel_id)
    return {"ok": True, "owner_account_id": None}


@router.post("/channels/{channel_id}/toggle")
def production_toggle(channel_id: int, payload: dict[str, Any], x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    enabled = bool(payload.get("enabled"))
    con = db_connect(False)
    try:
        cur = con.execute("UPDATE channels SET status=? WHERE id=?", (1 if enabled else 2, channel_id))
        if cur.rowcount != 1:
            raise HTTPException(status_code=404, detail="渠道不存在")
        con.commit()
    finally:
        con.close()
    return {"ok": True, "id": channel_id, "enabled": enabled}


@router.post("/channels")
def add_channel(payload: dict[str, Any], x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    name = str(payload.get("name", "")).strip()
    base = str(payload.get("base_url", "")).strip()
    key = str(payload.get("key", "")).strip()
    if not name or not base or not key or not base.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="名称、上游地址和 Key 必填")
    owner_id = str(payload.get("owner_account_id") or "").strip()
    if owner_id:
        from routes import _account_names
        if owner_id not in _account_names():
            raise HTTPException(status_code=400, detail="归属账号不存在")
    models = ",".join(str(payload.get("models", "")).split(","))
    con = db_connect(False)
    try:
        cur = con.execute(
            'INSERT INTO channels (type,key,name,status,weight,base_url,models,"group",priority,auto_ban,test_model) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (1, key, name, 1, 1, base, models, str(payload.get("group", "default")), int(payload.get("priority", 0)), 1, str(payload.get("test_model", ""))),
        )
        con.commit()
        if owner_id:
            from routes import set_channel_ownership
            set_channel_ownership(cur.lastrowid, owner_id, "manual")
        return {"ok": True, "id": cur.lastrowid}
    finally:
        con.close()


@router.patch("/channels/{channel_id}")
def edit_channel(channel_id: int, payload: dict[str, Any], x_channel_monitor_token: str | None = Header(default=None), authorization: str | None = Header(default=None)) -> dict[str, Any]:
    check_token(token_value(x_channel_monitor_token, authorization))
    allowed = {"name": "name", "base_url": "base_url", "models": "models", "group": '"group"', "priority": "priority", "test_model": "test_model"}
    updates, values = [], []
    for key, column in allowed.items():
        if key not in payload:
            continue
        value = payload[key]
        if key == "models":
            value = ",".join(str(value).split(","))
        if key == "priority":
            try:
                value = int(value)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="priority 必须是整数")
        updates.append(f"{column}=?")
        values.append(value)
    con = db_connect(False)
    try:
        if updates:
            values.append(channel_id)
            cur = con.execute(f"UPDATE channels SET {','.join(updates)} WHERE id=?", values)
            if cur.rowcount != 1:
                raise HTTPException(status_code=404, detail="渠道不存在")
            con.commit()
        elif not con.execute("SELECT 1 FROM channels WHERE id=?", (channel_id,)).fetchone():
            raise HTTPException(status_code=404, detail="渠道不存在")
    finally:
        con.close()
    if "owner_account_id" in payload:
        from routes import clear_channel_ownership, set_channel_ownership
        owner = payload.get("owner_account_id")
        if owner in (None, ""):
            clear_channel_ownership(channel_id)
        else:
            set_channel_ownership(channel_id, str(owner), "manual")
    return {"ok": True, "id": channel_id}


async def monitor_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            if get_monitor_global_enabled():
                for channel in list_channels():
                    if channel["monitor_enabled"] and channel["category_enabled"]:
                        result = await asyncio.to_thread(run_check, channel)
                        persist_check(channel["id"], result)
        except Exception:
            # One bad upstream must not stop the merged Hub monitor.
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=60)
        except asyncio.TimeoutError:
            continue


async def start_monitor_task() -> tuple[asyncio.Event, asyncio.Task[None]]:
    stop_event = asyncio.Event()
    return stop_event, asyncio.create_task(monitor_loop(stop_event), name="channel-monitor")
