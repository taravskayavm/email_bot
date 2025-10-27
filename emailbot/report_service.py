"""Helpers for building delivery reports."""

from __future__ import annotations

import csv
import datetime as dt
import json
import os
import sys
from itertools import chain
from pathlib import Path
from typing import Iterator

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:  # pragma: no cover - legacy fallback for Python < 3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore

SENT_LOG_PATH = Path(os.path.expanduser(os.getenv("SENT_LOG_PATH", "var/sent_log.csv")))
SEND_STATS_PATH = Path(os.path.expanduser(os.getenv("SEND_STATS_PATH", "var/send_stats.jsonl")))
REPORT_TZ_NAME = (os.getenv("REPORT_TZ") or "Europe/Moscow").strip() or "Europe/Moscow"
REPORT_TZ = ZoneInfo(REPORT_TZ_NAME)

_SUCCESS_STATUSES = {"sent", "success", "ok", "synced"}
_ERROR_STATUSES = {
    "failed",
    "fail",
    "error",
    "bounce",
    "bounced",
    "soft_bounce",
    "soft-bounce",
    "hard_bounce",
    "hard-bounce",
}


def _parse_ts_any(raw: str) -> dt.datetime | None:
    """Parse various ISO8601 timestamps and normalise them to UTC."""

    value = raw.strip()
    if not value:
        return None
    # Replace trailing "Z" to maintain compatibility with ``fromisoformat``
    normalized = value.replace("Z", "+00:00")
    try:
        ts = dt.datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    else:
        ts = ts.astimezone(dt.timezone.utc)
    return ts


def _detect_delimiter(sample: str) -> str:
    semi = sample.count(";")
    comma = sample.count(",")
    return ";" if semi > comma else ","


def _iter_sent_log() -> Iterator[dict[str, object]]:
    """Yield records from ``sent_log.csv`` regardless of schema version."""

    path = SENT_LOG_PATH
    if not path.exists():
        return

    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            sample = handle.read(1024)
            handle.seek(0)
            delimiter = _detect_delimiter(sample)
            reader = csv.DictReader(handle, delimiter=delimiter)
            for row in reader:
                if not row:
                    continue
                ts_raw = (row.get("last_sent_at") or row.get("ts") or "").strip()
                ts_utc = _parse_ts_any(ts_raw)
                if not ts_utc:
                    continue
                status_raw = (row.get("status") or "").strip()
                status = status_raw.lower() if status_raw else "sent"
                yield {"ts_utc": ts_utc, "status": status}
    except FileNotFoundError:
        return


def _iter_send_stats() -> Iterator[dict[str, object]]:
    """Yield records from ``send_stats.jsonl`` if available."""

    path = SEND_STATS_PATH
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            payload = line.strip()
            if not payload:
                continue
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                continue
            ts_raw = str(data.get("ts") or "").strip()
            ts_utc = _parse_ts_any(ts_raw)
            if not ts_utc:
                continue
            status_raw = data.get("status")
            if status_raw:
                status = str(status_raw).strip().lower()
            else:
                status = "sent" if data.get("success") else "failed"
            yield {"ts_utc": ts_utc, "status": status}



def summarize_day_local(today_local: dt.date | None = None) -> tuple[int, int]:
    """Return counts of successful and failed deliveries for the local day."""

    if today_local is None:
        today_local = dt.datetime.now(REPORT_TZ).date()
    start_local = dt.datetime.combine(today_local, dt.time(0, 0, 0), tzinfo=REPORT_TZ)
    end_local = start_local + dt.timedelta(days=1)

    ok = 0
    err = 0
    for item in chain(_iter_sent_log(), _iter_send_stats()):
        ts_utc = item["ts_utc"]
        if not isinstance(ts_utc, dt.datetime):
            continue
        status = str(item.get("status", "")).strip().lower()
        ts_local = ts_utc.astimezone(REPORT_TZ)
        if not (start_local <= ts_local < end_local):
            continue
        if status in _SUCCESS_STATUSES:
            ok += 1
        elif status in _ERROR_STATUSES:
            err += 1
    return ok, err


__all__ = [
    "REPORT_TZ",
    "REPORT_TZ_NAME",
    "SENT_LOG_PATH",
    "SEND_STATS_PATH",
    "summarize_day_local",
]
