from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

__all__ = [
    "LOCAL_TZ",
    "parse_timestamp_any",
    "parse_user_date_once",
    "day_bounds",
    "parse_ts",
]

_TZ = os.getenv("EMAILBOT_TZ", "Europe/Moscow")
LOCAL_TZ = ZoneInfo(_TZ)

_re_iso_date = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_re_ru_date = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")


def parse_user_date_once(value: str) -> tuple[datetime, datetime, str]:
    """Parse a single calendar date supplied by the user."""

    text = (value or "").strip()
    if _re_iso_date.fullmatch(text):
        naive = datetime.strptime(text, "%Y-%m-%d")
    elif _re_ru_date.fullmatch(text):
        naive = datetime.strptime(text, "%d.%m.%Y")
    else:
        raise ValueError("invalid date format")

    start = datetime(naive.year, naive.month, naive.day, 0, 0, 0, tzinfo=LOCAL_TZ)
    end = start + timedelta(days=1) - timedelta(microseconds=1)
    return start, end, naive.strftime("%d.%m.%Y")


def day_bounds(dt: datetime) -> tuple[datetime, datetime]:
    """Return start/end timestamps for the day of ``dt`` in :data:`LOCAL_TZ`."""

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    else:
        dt = dt.astimezone(LOCAL_TZ)
    start = datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=LOCAL_TZ)
    end = start + timedelta(days=1) - timedelta(microseconds=1)
    return start, end


def parse_ts(ts: str | datetime | None) -> datetime | None:
    """Parse ISO8601 timestamps to :data:`LOCAL_TZ`, handling ``Z`` suffix."""

    if ts is None:
        return None
    if isinstance(ts, datetime):
        dt = ts
    else:
        text = str(ts).strip()
        if not text:
            return None
        try:
            if text.endswith("Z"):
                dt = datetime.fromisoformat(text[:-1])
                dt = dt.replace(tzinfo=ZoneInfo("UTC"))
            else:
                dt = datetime.fromisoformat(text)
        except Exception:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(LOCAL_TZ)


def parse_timestamp_any(raw: str | None) -> datetime | None:
    """Parse ISO8601 timestamps with optional timezone information."""

    return parse_ts(raw)
