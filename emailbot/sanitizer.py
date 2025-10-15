from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import Iterable, List

from .cooldown import normalize_email

# Characters that should be removed before any filtering steps.
_HIDDEN_CHARS = [
    "\u200b",  # ZERO WIDTH SPACE
    "\u200c",  # ZERO WIDTH NON-JOINER
    "\u200d",  # ZERO WIDTH JOINER
    "\u200e",  # LEFT-TO-RIGHT MARK
    "\u200f",  # RIGHT-TO-LEFT MARK
    "\ufeff",  # ZERO WIDTH NO-BREAK SPACE (BOM)
    "\u00ad",  # SOFT HYPHEN
]

_REMOVE_TRANSLATION = {ord(ch): None for ch in _HIDDEN_CHARS}


@dataclass(slots=True)
class SanitizedBatch:
    """Container with pre-filtered addresses and helper metadata."""

    emails: List[str]
    normalized: dict[str, str]
    duplicates: int
    duplicate_items: List[str]


def _clean_display(value: str) -> str:
    """Return ``value`` stripped from invisibles while preserving the local part."""

    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = text.translate(_REMOVE_TRANSLATION)
    text = text.strip().strip(",;")
    if not text:
        return ""
    if "@" not in text:
        return text
    local_raw, _, domain_raw = text.partition("@")
    local = "".join(local_raw.split())
    domain = "".join(domain_raw.split())
    if not domain:
        return local
    try:
        domain_ascii = domain.encode("idna").decode("ascii")
    except Exception:
        domain_ascii = domain
    return f"{local}@{domain_ascii.lower()}"


def sanitize_batch(emails: Iterable[str]) -> SanitizedBatch:
    """Prepare ``emails`` for filtering by removing noise and duplicates early."""

    cleaned: list[str] = []
    normalized_map: dict[str, str] = {}
    seen_norms: set[str] = set()
    duplicate_items: list[str] = []
    duplicates = 0

    for raw in emails or []:
        display = _clean_display(raw)
        if not display:
            continue
        norm = normalize_email(display)
        key = norm or display.lower()
        if key in seen_norms:
            duplicates += 1
            duplicate_items.append(display)
            continue
        seen_norms.add(key)
        cleaned.append(display)
        normalized_map[display] = norm

    return SanitizedBatch(cleaned, normalized_map, duplicates, duplicate_items)


__all__ = ["SanitizedBatch", "sanitize_batch"]
