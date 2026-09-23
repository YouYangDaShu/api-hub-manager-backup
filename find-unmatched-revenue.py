import json
import sqlite3
from collections import defaultdict
from datetime import timedelta

import routes

accounts = routes._load_accounts()
manual = routes._load_channel_ownership()
account_ids = {str(a.get("id")) for a in accounts} | {routes.SELF_POOL_ACCOUNT_ID}
manual_owner = {
    int(cid): str(row.get("owner_account_id") or "")
    for cid, row in manual.items() if str(cid).isdigit()
}
manual_by_account = defaultdict(list)
for cid, owner in manual_owner.items():
    if owner in account_ids:
        manual_by_account[owner].append(cid)

static_owner = {}
for aid, ids in routes.SITE_CHANNEL_IDS_BY_ACCOUNT.items():
    for cid in ids:
        static_owner[int(cid)] = str(aid)

# Accounts with any explicit channel mapping do not use key fallback in production logic.
explicit_ids_by_account = {}
for aid in account_ids:
    static_ids = list(routes.SITE_CHANNEL_IDS_BY_ACCOUNT.get(aid, ()))
    manual_ids = manual_by_account.get(aid, [])
    if aid == routes.SELF_POOL_ACCOUNT_ID:
        explicit_ids_by_account[aid] = tuple(dict.fromkeys(manual_ids))
    else:
        explicit_ids_by_account[aid] = tuple(dict.fromkeys(static_ids + manual_ids)) if (static_ids or manual_ids) else None

key_fallback = defaultdict(list)
for a in accounts:
    aid = str(a.get("id"))
    key = str(a.get("upstream_key") or "")
    if key and explicit_ids_by_account.get(aid) is None:
        key_fallback[key].append(aid)

now = routes._site_revenue_now()
start = now.replace(hour=0, minute=0, second=0, microsecond=0)
end = start + timedelta(days=1)
overrides = routes.SITE_DAILY_CHANNEL_ATTRIBUTION_OVERRIDES.get(now.date().isoformat(), {})

con = sqlite3.connect(f"file:{routes.SITE_BILLING_DB}?mode=ro", uri=True)
qpu = float(con.execute("SELECT value FROM options WHERE key='QuotaPerUnit'").fetchone()[0])
rows = con.execute("""
SELECT l.channel_id, COALESCE(c.name, '[deleted]'), c.key,
       COUNT(*), SUM(l.quota), MIN(l.created_at), MAX(l.created_at)
FROM logs l LEFT JOIN channels c ON c.id=l.channel_id
WHERE l.type=2 AND l.quota>0 AND l.created_at>=? AND l.created_at<?
GROUP BY l.channel_id, c.name, c.key ORDER BY SUM(l.quota) DESC
""", (int(start.timestamp()), int(end.timestamp()))).fetchall()

unmatched=[]
for cid,name,key,count,quota,first,last in rows:
    owner=""
    source=""
    if cid in overrides:
        owner=str(overrides[cid]); source="daily_override"
    elif cid in manual_owner and manual_owner[cid] in account_ids:
        owner=manual_owner[cid]; source="manual"
    elif cid in static_owner and manual_owner.get(cid, static_owner[cid]) == static_owner[cid]:
        owner=static_owner[cid]; source="static_channel_id"
    elif key and key in key_fallback:
        owner=key_fallback[key][0]; source="exact_key"
    if not owner:
        unmatched.append((cid,name,count,quota/qpu,first,last, "manual="+manual_owner.get(cid,"none")))

print("date", now.date().isoformat(), "qpu", qpu)
print("unmatched_count",len(unmatched),"sum",round(sum(x[3] for x in unmatched),8))
for x in unmatched:
    print({"channel_id":x[0],"name":x[1],"requests":x[2],"amount":round(x[3],8),"first":x[4],"last":x[5],"state":x[6]})
