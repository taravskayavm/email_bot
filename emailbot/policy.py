"""Centralised decision making for outbound e-mails."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Callable

from emailbot.validators import is_valid_email, is_role_like
from emailbot.suppress_list import is_blocked
from emailbot import ledger

logger = logging.getLogger(__name__)

try:  # pragma: no cover - optional domain policy hook
    from emailbot.domain_policy import (  # type: ignore
        violates_domain_policy as _domain_policy_check,
    )
except Exception:  # pragma: no cover - fallback when module is absent

    def _domain_policy_check(email: str) -> bool:
        return False


_DOMAIN_POLICY_CHECKER: Callable[[str], bool] = _domain_policy_check


class Decision(Enum):
    SKIP_INVALID = auto()
    SKIP_ROLE = auto()
    SKIP_BLOCKED = auto()
    SKIP_DOMAIN_POLICY = auto()
    SKIP_COOLDOWN = auto()
    SEND_NOW = auto()


def _normalize(email: str) -> str:
    return (email or "").strip().lower()


def _ensure_aware(dt: datetime | None) -> datetime:
    value = dt or datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def violates_domain_policy(email: str) -> bool:
    """Return ``True`` when ``email`` violates a domain-level policy."""

    try:
        return bool(_DOMAIN_POLICY_CHECKER(email))
    except Exception:  # pragma: no cover - defensive against user hooks
        logger.debug("domain policy check failed for %s", email, exc_info=True)
        return False


def decide(email: str, campaign: str, now: datetime | None) -> tuple[Decision, str]:
    """Return the decision for ``email`` within ``campaign`` at ``now``."""

    normalized = _normalize(email)
    if not is_valid_email(normalized):
        return Decision.SKIP_INVALID, "invalid"
    if is_role_like(normalized):
        return Decision.SKIP_ROLE, "role_like"
    if is_blocked(normalized):
        return Decision.SKIP_BLOCKED, "blocked"
    if violates_domain_policy(normalized):
        return Decision.SKIP_DOMAIN_POLICY, "domain_policy"
    moment = _ensure_aware(now)
    if not ledger.can_send(normalized, campaign, moment):
        return Decision.SKIP_COOLDOWN, "cooldown"
    return Decision.SEND_NOW, "ok"


def decide_with_reason(
    email: str,  # Рассматриваемый адрес для проверки
    *,
    campaign: str = "manual",  # Используем кампанию по умолчанию для ручной отправки
    now: datetime | None = None,  # Позволяем передать конкретный момент времени
) -> tuple[bool, str]:
    """Return boolean decision with textual reason, reusing the detailed pipeline."""

    decision, reason = decide(email, campaign, now)  # Запускаем основной конвейер принятия решения
    return decision == Decision.SEND_NOW, reason  # Возвращаем булев исход и причину отказа/успеха


__all__ = ["Decision", "decide", "decide_with_reason", "violates_domain_policy"]

