"""Service-level helpers for send history storage."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Iterable

from .extraction_common import normalize_email as _normalize_email
from . import history_store

_LOCK = Lock()
_INITIALIZED_PATH: Path | None = None
_DEFAULT_DB_PATH = Path("var/state.db")


def _resolve_path() -> Path:
    raw = os.getenv("HISTORY_DB_PATH")
    if raw:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = Path.cwd() / path
        return path
    return _DEFAULT_DB_PATH


def ensure_initialized() -> None:
    """Initialise the underlying SQLite database if needed."""

    global _INITIALIZED_PATH
    path = _resolve_path()
    with _LOCK:
        if _INITIALIZED_PATH != path:
            history_store.init_db(path)
            _INITIALIZED_PATH = path


def _canonical_email(email: str) -> str:
    email = (email or "").strip()
    if not email:
        return ""
    try:
        return _normalize_email(email)
    except Exception:
        return email.lower()


def _canonical_group(group: str) -> str:
    return (group or "").strip().lower()


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_days_rule_default() -> int:
    """Return the default number of days for the history rule."""

    for name in ("DAYS_RULE_DEFAULT", "EMAIL_LOOKBACK_DAYS"):
        raw = os.getenv(name)
        if raw is None:
            continue
        raw = raw.strip()
        if not raw:
            continue
        try:
            value = int(raw)
        except ValueError:
            continue
        return max(0, value)
    return 180


def mark_sent(email: str, group: str, msg_id: str | None, sent_at: datetime) -> None:
    """Record a successful send event."""

    ensure_initialized()
    norm_email = _canonical_email(email)
    if not norm_email:
        return
    norm_group = _canonical_group(group)
    history_store.record_sent(norm_email, norm_group, msg_id, _ensure_utc(sent_at))


def was_sent_within_days(email: str, group: str, days: int) -> bool:
    """Return ``True`` if the address was contacted within ``days`` days."""

    ensure_initialized()
    if days <= 0:
        return False
    norm_email = _canonical_email(email)
    if not norm_email:
        return False
    norm_group = _canonical_group(group)
    return history_store.was_sent_within(norm_email, norm_group, days)


def get_last_sent(email: str, group: str) -> datetime | None:
    """Return the last send timestamp for the address/group pair."""

    ensure_initialized()
    norm_email = _canonical_email(email)
    if not norm_email:
        return None
    norm_group = _canonical_group(group)
    return history_store.get_last_sent(norm_email, norm_group)


def filter_by_days(
    emails: Iterable[str], group: str, days: int
) -> tuple[list[str], list[str]]:
    """Split ``emails`` into allowed and rejected based on the N-day rule."""

    ensure_initialized()
    if days <= 0:
        return list(emails), []
    norm_group = _canonical_group(group)
    allowed: list[str] = []
    rejected: list[str] = []
    cache: dict[str, bool] = {}
    for email in emails:
        norm_email = _canonical_email(email)
        if not norm_email:
            allowed.append(email)
            continue
        cached = cache.get(norm_email)
        if cached is None:
            cached = history_store.was_sent_within(norm_email, norm_group, days)
            cache[norm_email] = cached
        if cached:
            rejected.append(email)
        else:
            allowed.append(email)
    return allowed, rejected


__all__ = [
    "ensure_initialized",
    "mark_sent",
    "was_sent_within_days",
    "filter_by_days",
    "get_last_sent",
    "get_days_rule_default",
]
