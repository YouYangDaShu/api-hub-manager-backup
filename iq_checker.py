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

请写出详细的分析推导与计算过程，并在最后一行明确写出【最终答案：XX】。"""

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
                return bool(ch.get("channel_enabled") and ch.get("monitor_enabled") and ch.get("category_enabled") and (ch.get("monitor_global_enabled") is not False))
        return False
    except Exception:
        return False

def parse_answer(text: str) -> int | None:
    if not text:
        return None
    # 1. 优先匹配明确的“最终答案”字样（支持加粗、括号、中英冒号、等号、中文介词等各种变体）
    m = re.search(r"最终答案[^\d\n\r]{0,12}(\d{1,3})", text)
    if m:
        return int(m.group(1))
    # 2. 匹配“答案是/为/：XX”或“最少取出/摸出/需要 XX 个/颗”
    m_near = re.search(r"(?:最少|至少|需要|取出|摸出|拿出|答案)[^\d\n\r]{0,10}(\d{1,3})[^\d\n\r]{0,4}(?:个|颗|糖)?", text[-400:])
    if m_near:
        val = int(m_near.group(1))
        if val in (21, 29):
            return val
    # 3. 针对 21 或 29 的反向优先扫描（在末尾 400 字符内寻找 21 或 29，避免 \b 在中文失效）
    m_candidates = re.findall(r"(?<!\d)(21|29)(?!\d)", text[-400:])
    if m_candidates:
        return int(m_candidates[-1])
    # 4. 兜底：抓取末尾最后出现的 1~3 位独立整数
    m_fallback = re.findall(r"(?<!\d)(\d{1,3})(?!\d)", text[-150:])
    if m_fallback:
        return int(m_fallback[-1])
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
    # 2. 'high' (fallback thinking effort if 'max' not supported or fails)
    # 3. 'standard' (fallback for models that reject reasoning_effort or temperature)
    # 给足长推理充分的推导时间（240s）
    tiers = [
        {"name": "max", "extra": {"reasoning_effort": "max", "max_completion_tokens": 4096}, "timeout": 240},
        {"name": "high", "extra": {"reasoning_effort": "high", "max_completion_tokens": 4096}, "timeout": 240},
        {"name": "standard", "extra": {"max_tokens": 4096, "temperature": 0.0}, "timeout": 180}
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
            "stream": True,
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
                stream_content = []
                stream_reasoning = []
                stream_usage = {}
                for raw_line in resp:
                    line = raw_line.decode(errors="ignore").strip()
                    if not line:
                        continue
                    if line.startswith("data: "):
                        data_str = line[6:].strip()
                        if data_str == "[DONE]":
                            break
                        try:
                            chunk = json.loads(data_str)
                            if "usage" in chunk and chunk["usage"]:
                                stream_usage = chunk["usage"]
                            c_list = chunk.get("choices", [])
                            if c_list:
                                delta = c_list[0].get("delta", {})
                                c_piece = delta.get("content") or ""
                                if c_piece:
                                    stream_content.append(str(c_piece))
                                r_piece = delta.get("reasoning_content") or delta.get("reasoning") or delta.get("thought") or ""
                                if r_piece:
                                    stream_reasoning.append(str(r_piece))
                        except Exception:
                            continue
                used_effort = tier["name"]
                data = {
                    "choices": [{
                        "message": {
                            "content": "".join(stream_content),
                            "reasoning_content": "".join(stream_reasoning)
                        }
                    }],
                    "usage": stream_usage
                }
                break
        except urllib.error.HTTPError as he:
            err_body = he.read().decode(errors="ignore")
            last_err_msg = f"HTTP {he.code}: {err_body[:100]}"
            # If 400 Bad Request or 502 Upstream/Gateway error during thinking, downgrade tier
            if he.code in (400, 502) and idx < len(tiers) - 1:
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
        choices = data.get("choices", [])
        msg = choices[0].get("message", {}) if choices else {}
        raw_content = msg.get("content") or (choices[0].get("text") if choices else "") or ""
        if isinstance(raw_content, list):
            content = "".join([b.get("text", "") for b in raw_content if isinstance(b, dict) and b.get("type") == "text"])
        else:
            content = str(raw_content)

        raw_reasoning = msg.get("reasoning_content") or msg.get("reasoning") or msg.get("thought") or ""
        if isinstance(raw_reasoning, list):
            reasoning = "".join([str(b) for b in raw_reasoning])
        else:
            reasoning = str(raw_reasoning)

        full_text = content
        if reasoning and reasoning.strip():
            full_text = f"[思考过程]\n{reasoning.strip()}\n\n[最终回答]\n{content.strip()}"
        elif not full_text:
            full_text = reasoning or ""

        ans = parse_answer(full_text)
        if ans == 21:
            status = "green"
        elif ans == 29:
            status = "yellow"
        else:
            status = "red"

        usage = data.get("usage", {})
        res = {
            "ok": True,
            "model": model,
            "answer": ans,
            "status": status,
            "duration": dur,
            "effort": used_effort,
            "full_text": full_text,
            "summary": content[-150:].strip() if content else (reasoning[-150:].strip() if reasoning else ""),
            "prompt_tokens": usage.get("prompt_tokens"),
            "completion_tokens": usage.get("completion_tokens"),
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
            "full_text": f"请求失败: {last_err_msg}",
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
    if model:
        ch_map["selected_model"] = model
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
