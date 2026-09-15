# Hub网页源码-20260827

这是从生产Hub `/opt/api-hub/app` 导出的、已脱敏的源码备份。

## 本地启动（Windows）

```bat
cd /d C:\Users\19757\Desktop\Hub网页源码-20260827
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python main.py
```

访问：`http://127.0.0.1:8899`

也可以双击 `start.bat`（前提是依赖已安装）。

## 数据与配置

- `data/accounts.json` 和 `data/settings.json` 是空白示例，不含生产账号或密钥。
- 生产数据库、账号凭据、Cookie、Token、API Key、缓存、账目和运行日志没有导出。
- `.env` 只保留变量名，实际运行时请按本机环境填写。
- `data/channel_mapping.json` 是渠道ID映射配置，不包含 API Key。

## 说明

- 这是源码备份，不是生产数据备份。
- 修改代码前请先复制一份当前目录作为本地回滚副本。
- Windows上运行时，`NEWAPI_DB` 等路径需要改成Windows本机可访问的数据库路径；没有数据库时可先运行页面和非数据功能。
