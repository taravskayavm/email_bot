"""Shim: unified cooldown API re-exported from :mod:`emailbot.services.cooldown`.

This module intentionally re-exports the canonical cooldown implementation to
avoid logic drift across different code paths (bulk/manual/preview).
"""
from __future__ import annotations

from .services.cooldown import (  # noqa: F401
    APPEND_TO_SENT,
    COOLDOWN_DAYS,
    COOLDOWN_WINDOW_DAYS,
    SENT_MAILBOX,
    CooldownHit,
    CooldownService,
    _load_history_from_csv,
    _load_history_from_db,
    _merged_history_map,
    audit_emails,
    build_cooldown_service,
    check_email,
    get_last_sent_at,
    is_under_cooldown,
    mark_sent,
    normalize_email,
    normalize_email_for_key,
    should_skip_by_cooldown,
    was_sent_recently,
)

__all__ = [
    "APPEND_TO_SENT",
    "COOLDOWN_DAYS",
    "COOLDOWN_WINDOW_DAYS",
    "SENT_MAILBOX",
    "CooldownHit",
    "CooldownService",
    "_load_history_from_csv",
    "_load_history_from_db",
    "_merged_history_map",
    "audit_emails",
    "build_cooldown_service",
    "check_email",
    "get_last_sent_at",
    "is_under_cooldown",
    "mark_sent",
    "normalize_email",
    "normalize_email_for_key",
    "should_skip_by_cooldown",
    "was_sent_recently",
]
