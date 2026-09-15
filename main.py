"""
中转站渠道整合管理工具
支持 NewAPI / Sub2API 类型的中转站账号管理
"""
import asyncio
import contextlib
import json
import os
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from routes import router, auto_refresh_loop, dashboard_cache_refresh_loop
from channel_monitor import router as channel_monitor_router, start_monitor_task

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ========== 启动时备份关键数据 ==========
    import shutil
    from datetime import datetime

    backup_dir = DATA_DIR / "backups"
    backup_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_files = ["accounts.json", "settings.json", "provision_mappings.json", "dashboard_cache.json"]

    backed_up = []
    for filename in backup_files:
        src = DATA_DIR / filename
        if src.exists():
            dst = backup_dir / f"{filename}.{timestamp}.bak"
            try:
                shutil.copy2(src, dst)
                backed_up.append(filename)
            except Exception as e:
                print(f"⚠️  备份 {filename} 失败: {e}")

    if backed_up:
        print(f"✅ 已备份: {', '.join(backed_up)}")

    # 只保留最近 10 个备份
    for pattern in ["*.json.*.bak"]:
        backups = sorted(backup_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
        for old_backup in backups[10:]:
            try:
                old_backup.unlink()
            except Exception:
                pass
    # ========== 备份结束 ==========

    # 启动时确保数据文件存在
    accounts_file = DATA_DIR / "accounts.json"
    if not accounts_file.exists():
        accounts_file.write_text("[]", encoding="utf-8")
    # Token续期与仪表盘缓存刷新都在服务端运行，不依赖浏览器保持打开。
    token_refresh_task = asyncio.create_task(auto_refresh_loop())
    dashboard_refresh_task = asyncio.create_task(dashboard_cache_refresh_loop())
    monitor_stop, monitor_task = await start_monitor_task()
    try:
        yield
    finally:
        token_refresh_task.cancel()
        dashboard_refresh_task.cancel()
        monitor_stop.set()
        monitor_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await token_refresh_task
        with contextlib.suppress(asyncio.CancelledError):
            await dashboard_refresh_task
        with contextlib.suppress(asyncio.CancelledError):
            await monitor_task


app = FastAPI(title="中转站渠道整合管理", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(router, prefix="/api")
app.include_router(channel_monitor_router, prefix="/api")

templates = Jinja2Templates(directory="templates")


def _dashboard_bootstrap() -> dict:
    """Read the last safe dashboard snapshot for first-paint rendering."""
    cache_file = DATA_DIR / "dashboard_cache.json"
    try:
        payload = json.loads(cache_file.read_text(encoding="utf-8"))
        data = payload.get("data") or {}
        if isinstance(data, dict) and isinstance(data.get("accounts"), list):
            data["from_cache"] = True
            return data
    except (OSError, json.JSONDecodeError, TypeError):
        pass
    return {}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    response = templates.TemplateResponse(
        "index.html",
        {"request": request, "dashboard_bootstrap": _dashboard_bootstrap()},
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


if __name__ == "__main__":
    import uvicorn
    # reload=False：避免 Windows 上双进程 + 热重载导致的启动慢/请求卡住
    # 开发时如需热重载可改为 reload=True
    uvicorn.run("main:app", host="0.0.0.0", port=8899, reload=False)
