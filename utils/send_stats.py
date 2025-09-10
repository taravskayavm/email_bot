import json
import os
import datetime as dt
from pathlib import Path

_PATH = Path(os.getenv("SEND_STATS_PATH", "var/send_stats.jsonl"))
_PATH.parent.mkdir(parents=True, exist_ok=True)

def log_success(email: str, group: str) -> None:
    rec = {
        "ts": dt.datetime.utcnow().isoformat() + "Z",
        "date": dt.date.today().isoformat(),
        "email": email,
        "group": (group or "").strip().lower(),
        "status": "sent",
    }
    with _PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def summarize_today() -> dict:
    today = dt.date.today().isoformat()
    ok = 0
    errs = 0
    if not _PATH.exists():
        return {"ok": 0, "err": 0}
    with _PATH.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("date") == today:
                if rec.get("status") == "sent":
                    ok += 1
                elif rec.get("status") == "error":
                    errs += 1
    return {"ok": ok, "err": errs}

def log_error(email: str, group: str, reason: str) -> None:
    rec = {
        "ts": dt.datetime.utcnow().isoformat() + "Z",
        "date": dt.date.today().isoformat(),
        "email": email,
        "group": (group or "").strip().lower(),
        "status": "error",
        "reason": reason[:300],
    }
    with _PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
