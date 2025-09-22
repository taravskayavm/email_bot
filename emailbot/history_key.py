"""Utilities for normalising e-mail addresses for history lookups."""

from __future__ import annotations

import unicodedata

import idna

__all__ = ["normalize_history_key"]


def normalize_history_key(email: str) -> str:
    """Return a canonical key used for history deduplication."""

    text = unicodedata.normalize("NFKC", (email or "").strip().lower())
    if "@" not in text:
        return text
    local, domain = text.split("@", 1)
    try:
        domain_ascii = idna.encode(domain).decode("ascii")
    except Exception:
        domain_ascii = domain
    if domain_ascii in {"gmail.com", "googlemail.com"}:
        domain_ascii = "gmail.com"
        local = local.split("+", 1)[0].replace(".", "")
    return f"{local}@{domain_ascii}"
