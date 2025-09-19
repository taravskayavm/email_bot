from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from utils.paths import expand_path, ensure_parent

REPORT_TZ = os.getenv("REPORT_TZ", "Europe/Moscow")
HISTORY_PATH = expand_path(os.getenv("SEND_HISTORY_PATH", "var/send_history.jsonl"))
BLOCKLIST_PATH = expand_path(os.getenv("BLOCKLIST_PATH", "var/blocklist.txt"))
MONTHS_WINDOW = int(os.getenv("RULE_MONTHS_WINDOW", "6"))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ensure_dirs() -> None:
    ensure_parent(HISTORY_PATH)
    ensure_parent(BLOCKLIST_PATH)


def load_blocklist() -> set[str]:
    if not BLOCKLIST_PATH.exists():
        return set()
    try:
        return {
            line.strip().lower()
            for line in BLOCKLIST_PATH.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    except Exception:
        return set()


def is_blocked(addr: str) -> bool:
    return addr.strip().lower() in load_blocklist()


def append_history(addr: str) -> None:
    """Запоминаем успешную отправку (email + время UTC)."""

    ensure_dirs()
    record = {"email": addr.strip().lower(), "ts": _now_utc().isoformat()}
    with HISTORY_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def seen_within_window(addr: str, months: int | None = None) -> bool:
    """Проверка, было ли письмо этому адресу за последние N месяцев (по локальному журналу)."""

    months = months or MONTHS_WINDOW
    cutoff = _now_utc() - timedelta(days=30 * months)
    target = addr.strip().lower()
    if not target or not HISTORY_PATH.exists():
        return False
    try:
        with HISTORY_PATH.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                email = str(rec.get("email", "")).strip().lower()
                if email != target:
                    continue
                raw_ts = rec.get("ts")
                if not isinstance(raw_ts, str) or not raw_ts.strip():
                    continue
                try:
                    ts = datetime.fromisoformat(raw_ts)
                except Exception:
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                else:
                    ts = ts.astimezone(timezone.utc)
                if ts >= cutoff:
                    return True
        return False
    except Exception:
        return False
