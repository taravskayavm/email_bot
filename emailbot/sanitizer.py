from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable, List

ZWSP_CHARS = [
    "\u200b",  # ZERO WIDTH SPACE
    "\u200c",  # ZERO WIDTH NON-JOINER
    "\u200d",  # ZERO WIDTH JOINER
    "\u200e",  # LEFT-TO-RIGHT MARK
    "\u200f",  # RIGHT-TO-LEFT MARK
    "\ufeff",  # ZERO WIDTH NO-BREAK SPACE (BOM)
]
_ZW_RE = re.compile(r"[\u200B-\u200D\uFEFF]")
_WS_PUNCT_TRIM = re.compile(r"^[\s<>\[\]\(\)\.,;:\"']+|[\s<>\[\]\(\)\.,;:\"']+$")
_EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")

_OCR_EMAIL_FIXES = [
    (
        re.compile(
            r"([A-Za-z0-9._%+\-]+)\s*@\s*([A-Za-z0-9.\-]+)\s*\.\s*([A-Za-z]{2,})",
            re.IGNORECASE,
        ),
        lambda m: f"{m.group(1)}@{m.group(2)}.{m.group(3)}",
    ),
    (
        re.compile(
            r"(@[A-Za-z0-9.\-]+)\s*[·•∙⋅]?\s*\.\s*([A-Za-z]{2,})",
            re.IGNORECASE,
        ),
        lambda m: f"{m.group(1)}.{m.group(2)}",
    ),
    # пробел (и/или декоративная точка) вместо точки между доменом и TLD
    (
        re.compile(
            r"(@[A-Za-z0-9.\-]+)\s*(?:[·•∙⋅]\s*)?\s+([A-Za-z]{2,})\b",
            re.IGNORECASE,
        ),
        lambda m: f"{m.group(1)}.{m.group(2)}",
    ),
    # запятая вместо точки между доменом и TLD
    (
        re.compile(
            r"(@[A-Za-z0-9.\-]+)\s*,\s*([A-Za-z]{2,})\b",
            re.IGNORECASE,
        ),
        lambda m: f"{m.group(1)}.{m.group(2)}",
    ),
    (re.compile(r"\.\s*r\s*u\b", re.IGNORECASE), lambda m: ".ru"),
]


def heal_ocr_email_fragments(token: str) -> str:
    """Fix common OCR artefacts in ``token`` before validation."""

    s = token
    for rx, repl in _OCR_EMAIL_FIXES:
        s = rx.sub(repl, s)
    return s


def _join_linebreaks_around_dot(s: str) -> str:
    """Collapse line breaks accidentally inserted around domain dots."""

    return re.sub(r"\.\s*[\r\n]+\s*([A-Za-z]{2,})", r".\1", s)


def normalize_unicode(value: str) -> str:
    """Return ``value`` normalised with Unicode NFKC and without ZW chars."""

    if not isinstance(value, str):
        value = str(value)
    text = unicodedata.normalize("NFKC", value)
    return _ZW_RE.sub("", text)


def normalize_email(raw: str) -> str:
    """Normalise an address for deterministic comparisons and storage."""

    if not raw:
        return ""
    s = normalize_unicode(str(raw))
    s = _WS_PUNCT_TRIM.sub("", s)
    s = s.strip()
    if s:
        s = _join_linebreaks_around_dot(heal_ocr_email_fragments(s))
    if not s:
        return ""
    if "@" not in s:
        return s.lower()
    local_raw, _, domain_raw = s.partition("@")
    local = "".join(local_raw.split())
    domain = "".join(domain_raw.split())
    try:
        domain_ascii = domain.encode("idna").decode("ascii")
    except Exception:
        domain_ascii = domain
    if not domain_ascii:
        return local.lower()
    if not local:
        return f"@{domain_ascii.lower()}"
    return f"{local.lower()}@{domain_ascii.lower()}"


def email_key(raw: str) -> str:
    """Return a key used for deduplication / cooldown checks."""

    return normalize_email(raw)


def looks_like_email(raw: str) -> bool:
    """Best-effort heuristic detecting whether ``raw`` resembles an e-mail."""

    candidate = normalize_email(raw)
    return bool(candidate and _EMAIL_RE.match(candidate))


def dedup_emails(items: Iterable[str]) -> list[str]:
    """Return ``items`` without duplicates, keeping the first occurrence."""

    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        key = email_key(item)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


# Characters that should be removed before any filtering steps.
_HIDDEN_CHARS = [
    *ZWSP_CHARS,
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
    text = normalize_unicode(str(value))
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
        key = email_key(display) or display.lower()
        if key in seen_norms:
            duplicates += 1
            duplicate_items.append(display)
            continue
        seen_norms.add(key)
        cleaned.append(display)
        normalized_map[display] = norm

    return SanitizedBatch(cleaned, normalized_map, duplicates, duplicate_items)


__all__ = [
    "SanitizedBatch",
    "sanitize_batch",
    "normalize_unicode",
    "normalize_email",
    "email_key",
    "looks_like_email",
    "dedup_emails",
    "ZWSP_CHARS",
    "heal_ocr_email_fragments",
]
