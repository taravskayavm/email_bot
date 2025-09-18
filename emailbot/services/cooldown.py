"""Centralised utilities for enforcing the cooldown between sends."""

from __future__ import annotations

import email.utils
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

try:
    from emailbot.extraction_common import normalize_email as _canonical_normalize
except Exception:  # pragma: no cover - fallback if module layout changes
    _canonical_normalize = None


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip() or default)
    except Exception:
        return default


COOLDOWN_DAYS = _env_int("COOLDOWN_DAYS", 180)
SEND_STATS_PATH = os.getenv("SEND_STATS_PATH", "var/send_stats.jsonl")
APPEND_TO_SENT = os.getenv("APPEND_TO_SENT", "1") == "1"
SENT_MAILBOX = os.getenv("SENT_MAILBOX", "Отправленные")

_GMAIL_RE_PLUS = re.compile(r"^([^+]+)\+[^@]+(@gmail\.com)$", re.IGNORECASE)


def _send_stats_path() -> Path:
    raw = os.getenv("SEND_STATS_PATH", SEND_STATS_PATH)
    path = Path(str(raw)).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def _cooldown_days(days: Optional[int]) -> int:
    if days is not None:
        return days
    return _env_int("COOLDOWN_DAYS", COOLDOWN_DAYS)


def _append_to_sent_enabled() -> bool:
    flag = os.getenv("APPEND_TO_SENT")
    if flag is None:
        return APPEND_TO_SENT
    return str(flag).strip() == "1"


def _sent_mailbox() -> str:
    raw = os.getenv("SENT_MAILBOX")
    if raw is None:
        return SENT_MAILBOX
    value = str(raw).strip()
    return value or SENT_MAILBOX


def _gmail_canonical(local: str, domain: str) -> str:
    if not local:
        return f"@{domain.lower()}"
    local = local.replace(".", "")
    return f"{local}@{domain.lower()}"


def normalize_email_for_key(raw: str) -> str:
    """Return a canonical e-mail identifier suitable for cooldown lookups."""

    if not raw:
        return ""
    addr = email.utils.parseaddr(str(raw))[1].strip()
    if not addr:
        return ""

    addr = addr.lower()
    local, sep, domain = addr.partition("@")
    if not sep:
        return addr

    if domain in {"gmail.com", "googlemail.com"}:
        domain = "gmail.com"
        m = _GMAIL_RE_PLUS.match(addr)
        if m:
            local = m.group(1)
        addr = _gmail_canonical(local, domain)
        local, _, domain = addr.partition("@")

    if _canonical_normalize is not None:
        try:
            return _canonical_normalize(f"{local}@{domain}").lower()
        except Exception:
            pass
    return f"{local}@{domain}".lower()


def _iter_send_stats() -> Iterable[dict]:
    path = _send_stats_path()
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except Exception:  # pragma: no cover - malformed record
                    continue
    except FileNotFoundError:
        return []


def _last_from_send_stats(email_norm: str) -> Optional[datetime]:
    last: Optional[datetime] = None
    for rec in _iter_send_stats():
        e = normalize_email_for_key(rec.get("email", ""))
        if not e or e != email_norm:
            continue
        ts = rec.get("ts") or rec.get("timestamp") or rec.get("date")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if last is None or dt > last:
            last = dt
    return last


def _last_from_history(email_raw: str) -> tuple[Optional[datetime], Optional[str]]:
    try:
        from emailbot import history_service
    except Exception:
        return None, None

    try:
        history_service.ensure_initialized()
        info = history_service.get_last_sent_any_group(email_raw)
    except Exception:
        return None, None

    if not info:
        return None, None

    group, dt = info
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt, group or None


def _last_from_imap(email_norm: str) -> Optional[datetime]:
    if not _append_to_sent_enabled():
        return None

    host = os.getenv("IMAP_HOST")
    user = os.getenv("EMAIL_ADDRESS")
    password = os.getenv("EMAIL_PASSWORD")
    if not host or not user or not password:
        return None

    try:
        from emailbot.mail.imap_lookup import find_last_sent_at
    except Exception:  # pragma: no cover - optional dependency missing
        return None

    try:
        days = _cooldown_days(None)
        mailbox = _sent_mailbox()
        return find_last_sent_at(email_norm, mailbox, days=days)
    except Exception:  # pragma: no cover - IMAP issues
        return None


def _combine_last_times(
    email_raw: str, email_norm: str
) -> tuple[Optional[datetime], dict[str, Optional[str]]]:
    best: Optional[datetime] = None
    meta: dict[str, Optional[str]] = {"source": None, "group": None}

    hist_dt, hist_group = _last_from_history(email_raw)
    if hist_dt is not None:
        best = hist_dt
        meta = {"source": "history", "group": hist_group}

    for source, candidate in (
        ("send_stats", _last_from_send_stats(email_norm)),
        ("imap", _last_from_imap(email_norm)),
    ):
        if candidate is None:
            continue
        if candidate.tzinfo is None:
            candidate = candidate.replace(tzinfo=timezone.utc)
        else:
            candidate = candidate.astimezone(timezone.utc)
        if best is None or candidate > best:
            best = candidate
            meta = {"source": source, "group": None}

    return best, meta


def get_last_sent_at(email_raw: str) -> Optional[datetime]:
    key = normalize_email_for_key(email_raw)
    if not key:
        return None
    last, _ = _combine_last_times(email_raw, key)
    return last


def should_skip_by_cooldown(
    email_raw: str,
    now: Optional[datetime] = None,
    days: Optional[int] = None,
) -> tuple[bool, str]:
    if not email_raw:
        return False, ""

    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)

    key = normalize_email_for_key(email_raw)
    if not key:
        return False, ""

    window = _cooldown_days(days)
    last, meta = _combine_last_times(email_raw, key)
    if not last:
        return False, ""

    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    else:
        last = last.astimezone(timezone.utc)

    delta = now - last
    threshold = timedelta(days=window)
    if delta < threshold:
        remain = threshold - delta
        hours_left = int(remain.total_seconds() // 3600)
        parts = [f"cooldown<{window}d", f"last={last.isoformat()}"]
        source = meta.get("source")
        if source:
            parts.append(f"source={source}")
        group = meta.get("group")
        if group:
            parts.append(f"group={group}")
        parts.append(f"remain≈{hours_left}h")
        reason = "; ".join(parts)
        return True, reason
    return False, ""
