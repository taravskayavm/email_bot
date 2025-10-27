"""Persistence helpers for the send decision pipeline."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from emailbot.history_service import mark_sent as history_mark_sent
from emailbot.services.cooldown import (
    COOLDOWN_DAYS,
    mark_sent as cooldown_mark_sent,
    should_skip_by_cooldown,
)

logger = logging.getLogger(__name__)


def _ensure_aware(dt: datetime | None) -> datetime:
    value = dt or datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def can_send(email: str, campaign: str, now: datetime | None) -> bool:
    """Return ``True`` when ``email`` can be contacted right now."""

    del campaign  # campaign-specific throttling is not implemented yet
    moment = _ensure_aware(now)
    skip, _reason = should_skip_by_cooldown(email, now=moment, days=COOLDOWN_DAYS)
    return not skip


def record_send(
    email: str,
    campaign: str,
    now: datetime | None,
    *,
    message_id: str | None = None,
    run_id: str | None = None,
    smtp_result: str = "ok",
) -> None:
    """Persist the successful send event for cooldown bookkeeping."""

    timestamp = _ensure_aware(now)
    try:
        cooldown_mark_sent(email, sent_at=timestamp)
    except Exception:  # pragma: no cover - logging only
        logger.debug("cooldown mark_sent failed", exc_info=True)
    try:
        history_mark_sent(
            email,
            campaign,
            message_id,
            timestamp,
            run_id=run_id or "",
            smtp_result=smtp_result,
        )
    except Exception:  # pragma: no cover - logging only
        logger.debug("history mark_sent failed", exc_info=True)


__all__ = ["can_send", "record_send"]

