# /opt/api-hub/app/iq_checker.py
import asyncio
import json
import os
import re
import sqlite3
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

DATA_PATH = Path("/opt/api-hub/data/iq_check_state.json")
MONITOR_STATE_PATH = Path("/opt/api-hub/data/monitor-state.json")
DB_PATH = Path("/opt/new-api/data/one-api.db")

QUESTION = """在一个黑色的袋子里放有三种口味的糖果，每种糖果有两种不同的形状（圆形和五角星形，不同的形状靠手感可以分辨）。现已知不同口味的糖和不同形状的数量统计如下表。参赛者需要在活动前决定摸出的糖果数目，那么，最少取出多少个糖果才能保证手中同时拥有不同形状的苹果味和桃子味的糖？（同时手中有圆形苹果味匹配五角星桃子味糖果，或者有圆形桃子味匹配五角星苹果味糖果都满足要求）

口味/形状 苹果味 桃子味 西瓜味
圆形 7 9 8
五角星形 7 6 4

请直接给出最终数字答案，并在最后一行明确写出【最终答案：XX】。"""

def load_iq_state() -> dict[str, Any]:
    if not DATA_PATH.exists():
        return {"enabled": True, "channels": {}}
    try:
        with open(DATA_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"enabled": True, "channels": {}}

def save_iq_state(state: dict[str, Any]):
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = DATA_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp.replace(DATA_PATH)

def is_channel_monitored_in_availability(cid: int) -> bool:
    """跟随渠道可用性中的监控开关：如果渠道可用性里关闭了，这边也关；那边打开，这边也开始检测。"""
    try:
        import channel_monitor
        for ch in channel_monitor.list_channels():
            if int(ch.get("id")) == int(cid):
                return bool(ch.get("monitor_enabled", True) and ch.get("category_enabled", True) and (ch.get("monitor_global_enabled") is not False))
        return False
    except Exception:
        return True

def parse_answer(text: str) -> int | None:
    if not text:
        return None
    m = re.search(r"最终答案[：:\s]*(\d+)", text)
    if m:
        return int(m.group(1))
    m2 = re.findall(r"(21|29)", text)
    if m2:
        return int(m2[-1])
    m3 = re.findall(r"(\d{1,3})", text[-100:])
    if m3:
        return int(m3[-1])
    return None

def run_single_iq_test(cid: int, model: str | None = None) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    c = conn.cursor()
    c.execute('SELECT id, name, "key", base_url, models FROM channels WHERE id=?', (cid,))
    row = c.fetchone()
    conn.close()
    if not row:
        return {"ok": False, "error": "渠道不存在", "status": "red", "at": time.strftime("%Y-%m-%d %H:%M:%S")}

    cid, name, key, base, models_str = row
    models = [m.strip() for m in (models_str or "").split(",") if m.strip()]
    if not model:
        state = load_iq_state()
        ch_state = state.get("channels", {}).get(str(cid), {})
        model = ch_state.get("selected_model") or ("gpt-5.6-sol" if "gpt-5.6-sol" in models else (models[0] if models else "gpt-5.6-sol"))

    # Route through New API local gateway (127.0.0.1:3000) with Admin Token + Channel Pin
    # Key format: Bearer sk-<ADMIN_TOKEN>-<CHANNEL_ID>
    # This generates real audit logs & billing in New API for this exact channel!
    admin_token_key = ""
    try:
        conn_token = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        ct = conn_token.cursor()
        ct.execute("SELECT key FROM tokens WHERE user_id=1 AND name='admin' AND status=1 LIMIT 1")
        t_row = ct.fetchone()
        if not t_row:
            ct.execute("SELECT key FROM tokens WHERE user_id=1 AND status=1 LIMIT 1")
            t_row = ct.fetchone()
        if t_row:
            admin_token_key = t_row[0]
        conn_token.close()
    except Exception as te:
        logger.error(f"Failed to fetch admin token for gateway: {te}")

    if admin_token_key:
        url = "http://127.0.0.1:3000/v1/chat/completions"
        auth_header = f"Bearer sk-{admin_token_key}-{cid}"
    else:
        # Fallback to direct channel upstream if token not found
        url = f"{base.rstrip('/')}/v1/chat/completions"
        auth_header = f"Bearer {key}"

    # Adaptive thinking intensity tiers:
    # 1. 'max' (highest thinking effort, max_completion_tokens=4096)
    # 2. 'high' (fallback thinking effort if 'max' not supported or times out)
    # 3. 'standard' (fallback for models that reject reasoning_effort or temperature)
    # 给足长推理充分的推导时间（3分钟窗口，180s），避免提前切走中断深度思考
    tiers = [
        {"name": "max", "extra": {"reasoning_effort": "max", "max_completion_tokens": 4096}, "timeout": 180},
        {"name": "high", "extra": {"reasoning_effort": "high", "max_completion_tokens": 4096}, "timeout": 180},
        {"name": "standard", "extra": {"max_tokens": 4096, "temperature": 0.0}, "timeout": 120}
    ]

    t0 = time.time()
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    data = None
    used_effort = "standard"
    last_err_msg = ""

    for idx, tier in enumerate(tiers):
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": QUESTION}],
            **tier["extra"]
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": auth_header,
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Hub-IQ-Checker)"
            }
        )
        try:
            with urllib.request.urlopen(req, timeout=tier["timeout"]) as resp:
                data = json.loads(resp.read().decode())
                used_effort = tier["name"]
                break
        except urllib.error.HTTPError as he:
            err_body = he.read().decode(errors="ignore")
            last_err_msg = f"HTTP {he.code}: {err_body[:100]}"
            # If 400 Bad Request regarding reasoning_effort or unsupported options, downgrade
            if he.code == 400 and idx < len(tiers) - 1:
                continue
            break
        except Exception as te:
            last_err_msg = str(te)
            # If max effort timed out, try high effort
            if tier["name"] == "max" and idx < len(tiers) - 1:
                continue
            break

    dur = round(time.time() - t0, 2)
    if data:
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        ans = parse_answer(content)
        if ans == 21:
            status = "green"
        elif ans == 29:
            status = "yellow"
        else:
            status = "red"
        res = {
            "ok": True,
            "model": model,
            "answer": ans,
            "status": status,
            "duration": dur,
            "effort": used_effort,
            "summary": content[-150:].strip() if content else "",
            "at": now_str
        }
    else:
        res = {
            "ok": False,
            "model": model,
            "answer": None,
            "status": "red",
            "duration": dur,
            "effort": used_effort,
            "summary": f"请求失败: {last_err_msg[:100]}",
            "at": now_str
        }

    # Save to history (keep max 48 items)
    state = load_iq_state()
    ch_map = state.setdefault("channels", {}).setdefault(str(cid), {
        "enabled": True,
        "selected_model": model,
        "history": []
    })
    hist = ch_map.setdefault("history", [])
    hist.append(res)
    ch_map["history"] = hist[-48:]
    save_iq_state(state)
    return res

def seconds_until_next_hour() -> float:
    """计算距离下一个整点 (:00) 的剩余秒数"""
    now = datetime.now()
    target = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return max(1.0, (target - now).total_seconds())

async def iq_check_loop(stop_event: asyncio.Event):
    """固定对齐到每个整点 (:00) 自动执行检测（一小时一次）"""
    while not stop_event.is_set():
        wait_secs = seconds_until_next_hour()
        print(f"[iq_checker] Next check aligned to :00 sharp in {round(wait_secs, 1)}s")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=wait_secs)
            break
        except asyncio.TimeoutError:
            pass

        state = load_iq_state()
        if state.get("enabled", True):
            try:
                conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
                c = conn.cursor()
                c.execute("SELECT id, name, models FROM channels WHERE status=1")
                channels = c.fetchall()
                conn.close()

                for cid, name, models_str in channels:
                    if stop_event.is_set():
                        break
                    # 联动判断：必须渠道可用性里开启监控，且本渠道未显式禁用
                    if not is_channel_monitored_in_availability(cid):
                        continue
                    ch_conf = state.get("channels", {}).get(str(cid), {})
                    if ch_conf.get("enabled", True):
                        models = [m.strip() for m in (models_str or "").split(",") if m.strip()]
                        sel_model = ch_conf.get("selected_model") or ("gpt-5.6-sol" if "gpt-5.6-sol" in models else (models[0] if models else None))
                        if sel_model:
                            await asyncio.to_thread(run_single_iq_test, cid, sel_model)
                            await asyncio.sleep(2)
            except Exception as e:
                print(f"[iq_checker] Loop error: {e}")

async def start_iq_task() -> tuple[asyncio.Event, asyncio.Task[None]]:
    stop_event = asyncio.Event()
    task = asyncio.create_task(iq_check_loop(stop_event), name="iq-checker")
    return stop_event, task
