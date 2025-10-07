"""Utilities for normalising e-mail addresses for history lookups."""

from __future__ import annotations

import unicodedata

import idna

from utils.email_canonical import canonicalize_email

from emailbot.settings import (
    CANON_GMAIL_DOTS,
    CANON_GMAIL_PLUS,
    CANON_OTHER_PLUS,
    ENABLE_PROVIDER_CANON,
)

__all__ = ["normalize_history_key"]


def normalize_history_key(email: str) -> str:
    """Return a canonical key used for history deduplication."""

    raw = unicodedata.normalize("NFKC", (email or "").strip())
    if not raw:
        return ""
    lowered = raw.lower()
    if "@" not in lowered:
        return lowered
    local, domain = lowered.split("@", 1)
    try:
        domain_ascii = idna.encode(domain).decode("ascii")
    except Exception:
        domain_ascii = domain
    base = f"{local}@{domain_ascii}"
    canonical = canonicalize_email(
        base,
        gmail_dots=bool(ENABLE_PROVIDER_CANON and CANON_GMAIL_DOTS),
        gmail_plus=bool(ENABLE_PROVIDER_CANON and CANON_GMAIL_PLUS),
        other_plus=bool(ENABLE_PROVIDER_CANON and CANON_OTHER_PLUS),
    )
    return unicodedata.normalize("NFKC", canonical or base)
