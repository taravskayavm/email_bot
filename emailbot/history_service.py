"""Service-level helpers for send history storage."""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Iterable, List, Tuple

try:  # pragma: no cover - optional dependency in lightweight deployments
    import idna
except Exception:  # pragma: no cover - degrade gracefully when idna is absent
    idna = None  # type: ignore[assignment]

try:  # pragma: no cover - settings are optional during lightweight imports
    from . import settings  # type: ignore
except Exception:  # pragma: no cover - degrade gracefully when settings missing
    settings = None  # type: ignore[assignment]

from . import history_store
from emailbot.services.cooldown import _env_int
from utils.email_clean import normalize_email_unified
from utils.paths import expand_path, get_temp_dir

_LOCK = Lock()
_INITIALIZED_PATH: Path | None = None
_DEFAULT_DB_PATH = expand_path("var/state.db")


def _cooldown_days_default() -> int:
    try:
        raw = getattr(settings, "SEND_COOLDOWN_DAYS", 180)
        return int(raw)
    except Exception:
        return 180


COOLDOWN_DAYS = _env_int("COOLDOWN_DAYS", _cooldown_days_default())


_DT_CANDIDATES = (
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})?",
    r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",
    r"\d{4}-\d{2}-\d{2}",
)
_DT_REGEX = re.compile("|".join(f"(?:{pattern})" for pattern in _DT_CANDIDATES))


def _parse_dt(value) -> datetime | None:
    """Return a naive UTC datetime extracted from ``value``.

    ``value`` may contain an ISO formatted string, a date in ``YYYY-MM-DD`` format or
    an aware ``datetime`` instance.  The helper extracts the first matching pattern
    and normalises the timestamp to naive UTC to simplify timedelta calculations.
    """

    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value)
        match = _DT_REGEX.search(text)
        if not match:
            return None
        raw = match.group(0)
        cleaned = raw.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(cleaned)
        except Exception:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(raw, fmt)
                    break
                except Exception:
                    continue
            else:
                return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _default_db_path() -> Path:
    """Return default DB path, isolating pytest runs."""

    if os.getenv("PYTEST_CURRENT_TEST"):
        base = get_temp_dir("emailbot_test_state")
        return base / "state.db"
    return _DEFAULT_DB_PATH


def _resolve_path() -> Path:
    # 1) Явная переменная окружения
    raw = os.getenv("HISTORY_DB_PATH")
    if raw:
        return expand_path(raw)
    # 2) Настройки проекта (то, что печатается как HISTORY_DB=...)
    try:
        cfg = getattr(settings, "HISTORY_DB", None)
        if cfg:
            return expand_path(str(cfg))
    except Exception:
        pass
    # 3) Дефолт
    return _default_db_path()


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
        base = normalize_email_unified(email)
    except Exception:
        base = (email or "").strip().lower()

    if not base:
        return ""

    local, sep, domain = base.partition("@")
    if not sep:
        return base

    domain_norm = domain.strip()
    if idna is not None and domain_norm:
        try:
            domain_norm = idna.encode(domain_norm, uts46=True).decode("ascii")
        except Exception:
            domain_norm = domain_norm.lower()

    if not domain_norm:
        return base

    return f"{local}@{domain_norm.lower()}"


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


def register_send_attempt(
    email: str,
    group: str,
    *,
    days: int,
    sent_at: datetime | None = None,
    run_id: str = "",
    message_id: str = "",
) -> datetime | None:
    """Reserve a slot for sending while enforcing the cooldown window.

    Returns the timestamp used for the reservation if successful, or
    ``None`` if the cooldown constraint blocks the send attempt.
    """

    ensure_initialized()
    norm_email = _norm_email(email)
    if not norm_email:
        return _ensure_utc(sent_at)
    norm_group = _norm_group(group)
    ts = _ensure_utc(sent_at)
    if days <= 0:
        return ts
    ok = history_store.try_reserve_send(
        norm_email,
        norm_group,
        ts,
        cooldown=timedelta(days=days),
        message_id=(message_id or "").strip(),
        run_id=run_id,
        smtp_result="pending",
    )
    if not ok:
        return None
    return ts


def cancel_send_attempt(email: str, group: str, sent_at: datetime | None) -> None:
    """Rollback a previously reserved send attempt."""

    if sent_at is None:
        return
    ensure_initialized()
    norm_email = _norm_email(email)
    if not norm_email:
        return
    norm_group = _norm_group(group)
    history_store.delete_send_record(norm_email, norm_group, _ensure_utc(sent_at))


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


def get_last_sent_dt(email: str) -> datetime | None:
    """Return the last known send timestamp (any group) as naive UTC."""

    info = get_last_sent_any_group(email)
    if not info:
        return None
    _, last = info
    parsed = _parse_dt(last)
    return parsed


def can_send_now(email: str) -> bool:
    """Return ``True`` if ``email`` is outside the cooldown window."""

    last = get_last_sent_dt(email)
    if not last:
        return True
    window_days = get_days_rule_default()
    if window_days <= 0:
        return True
    now = datetime.utcnow()
    return (now - last) >= timedelta(days=window_days)


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
    "register_send_attempt",
    "cancel_send_attempt",
    "was_sent_within_days",
    "filter_by_days",
    "get_last_sent",
    "get_last_sent_any_group",
    "get_last_sent_dt",
    "can_send_now",
    "get_days_rule_default",
]
