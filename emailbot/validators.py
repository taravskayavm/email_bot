"""Validation helpers for outbound e-mail decisions."""

from __future__ import annotations

from typing import Callable

from utils.email_clean import is_valid_email as _legacy_is_valid_email

try:  # pragma: no cover - optional dependency for advanced heuristics
    from utils.email_role import classify_email_role as _classify_email_role
except Exception:  # pragma: no cover - degrade gracefully in minimal setups

    def _classify_email_role(local: str, domain: str, context_text: str = ""):
        return {"class": "unknown", "score": 0.5, "reason": "disabled"}


_ROLE_CHECKER: Callable[[str, str], bool]


def _role_checker(local: str, domain: str) -> bool:
    try:
        result = _classify_email_role(local, domain)
    except Exception:
        return False
    return str(result.get("class", "")).lower() == "role"


_ROLE_CHECKER = _role_checker


def is_valid_email(addr: str) -> bool:
    """Return ``True`` if ``addr`` is a syntactically valid e-mail address."""

    return _legacy_is_valid_email(addr)


def is_role_like(addr: str) -> bool:
    """Heuristically determine whether ``addr`` resembles a role account."""

    if not addr or "@" not in addr:
        return False
    local, domain = addr.split("@", 1)
    local = local.strip()
    domain = domain.strip()
    if not local or not domain:
        return False
    return _ROLE_CHECKER(local, domain)


__all__ = ["is_valid_email", "is_role_like"]

