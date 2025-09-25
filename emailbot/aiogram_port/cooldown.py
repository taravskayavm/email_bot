"""Lightweight wrapper around the existing cooldown service."""

from __future__ import annotations

from typing import Optional, Tuple

from emailbot.services import cooldown as cooldown_service


def enforce_cooldown(email: str, *, days: Optional[int] = None) -> Tuple[bool, Optional[str]]:
    """Check whether the cooldown allows sending to ``email`` now."""

    if not email:
        return False, "empty email"
    skip, reason = cooldown_service.should_skip_by_cooldown(email, days=days)
    if skip:
        return False, reason or "cooldown active"
    return True, None


def mark_sent(email: str) -> None:
    """Persist send information for subsequent cooldown checks."""

    cooldown_service.mark_sent(email)
