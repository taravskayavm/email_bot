"""Service-level helpers for send history storage."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Iterable, List, Tuple

from .extraction_common import normalize_email as _normalize_email
from . import history_store
from emailbot.services.cooldown import _env_int

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


def _norm_email(email: str) -> str:
    try:
        return _normalize_email(email)
    except Exception:
        return (email or "").strip().lower()


def _norm_group(group: str) -> str:
    return (group or "").strip().lower()


def _ensure_utc(dt: datetime | None) -> datetime:
    value = dt or datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def get_days_rule_default() -> int:
    """Return the default number of days for the history rule."""

    # Keep the default in sync with the global cooldown setting.
    return _env_int("COOLDOWN_DAYS", 180)


def mark_sent(
    email: str,
    group: str,
    msg_id: str | None = None,
    sent_at: datetime | None = None,
    *,
    run_id: str = "",
    smtp_result: str = "ok",
) -> None:
    """Record a successful send event."""

    ensure_initialized()
    norm_email = _norm_email(email)
    if not norm_email:
        return
    norm_group = _norm_group(group)
    ts = _ensure_utc(sent_at)
    history_store.record_send(
        norm_email,
        norm_group,
        ts,
        message_id=(msg_id or "").strip(),
        run_id=run_id,
        smtp_result=smtp_result,
    )


def was_sent_within_days(email: str, group: str, days: int) -> bool:
    """Return ``True`` if the address was contacted within ``days`` days."""

    ensure_initialized()
    if days <= 0:
        return False
    norm_email = _norm_email(email)
    if not norm_email:
        return False
    norm_group = _norm_group(group)
    last = history_store.last_send(norm_email, norm_group)
    if last is None:
        return False
    return (datetime.now(timezone.utc) - last) < timedelta(days=days)


def get_last_sent(email: str, group: str) -> datetime | None:
    """Return the last send timestamp for the address/group pair."""

    ensure_initialized()
    norm_email = _norm_email(email)
    if not norm_email:
        return None
    norm_group = _norm_group(group)
    return history_store.last_send(norm_email, norm_group)


def get_last_sent_any_group(email: str) -> Tuple[str, datetime] | None:
    """Return the most recent send timestamp regardless of group."""

    ensure_initialized()
    norm_email = _norm_email(email)
    if not norm_email:
        return None
    return history_store.last_send_any_group(norm_email)


def filter_by_days(
    emails: Iterable[str], group: str, days: int
) -> tuple[list[str], list[str]]:
    """Split ``emails`` into allowed and rejected based on the N-day rule."""

    ensure_initialized()
    if days <= 0:
        return list(emails), []
    norm_group = _norm_group(group)
    allowed: List[str] = []
    rejected: List[str] = []
    cache: dict[str, datetime | None] = {}
    threshold = datetime.now(timezone.utc) - timedelta(days=days)
    for email in emails:
        norm_email = _norm_email(email)
        if not norm_email:
            allowed.append(email)
            continue
        if norm_email not in cache:
            cache[norm_email] = history_store.last_send(norm_email, norm_group)
        last = cache[norm_email]
        if last and last >= threshold:
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
    "get_last_sent_any_group",
    "get_days_rule_default",
]
