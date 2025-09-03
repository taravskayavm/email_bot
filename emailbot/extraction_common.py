"""Common helpers for e-mail extraction and normalization."""
from __future__ import annotations

import re
import unicodedata

__all__ = ["normalize_text", "preprocess_text", "normalize_email"]

# Mapping of Cyrillic homoglyphs to their Latin counterparts
_CYR_TO_LATIN = str.maketrans({
    "а": "a",
    "е": "e",
    "о": "o",
    "р": "p",
    "с": "c",
    "х": "x",
    "А": "A",
    "Е": "E",
    "О": "O",
    "Р": "P",
    "С": "C",
    "Х": "X",
})

# Thin/zero-width spaces which should be treated as regular spaces
_Z_SPACE_RE = re.compile(r"[\u2000-\u200A\u202F\u205F\u3000]")


def normalize_text(s: str) -> str:
    """Return ``s`` normalized for e-mail extraction."""

    s = unicodedata.normalize("NFKC", s or "")
    s = s.translate(_CYR_TO_LATIN)

    # Spaces
    s = s.replace("\u00A0", " ")  # NBSP
    s = _Z_SPACE_RE.sub(" ", s)

    # Zero-width and soft hyphen characters
    s = (
        s.replace("\u200B", "")
        .replace("\u200C", "")
        .replace("\u200D", "")
        .replace("\uFEFF", "")
        .replace("\u00AD", "")
    )

    # Various dashes/minuses -> ASCII '-'
    s = (
        s.replace("\u2010", "-")
        .replace("\u2011", "-")
        .replace("\u2012", "-")
        .replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2015", "-")
        .replace("\u2212", "-")
        .replace("\u2043", "-")
        .replace("\uFE63", "-")
        .replace("\uFF0D", "-")
    )

    # Apostrophes/quotes -> ASCII "'"
    s = (
        s.replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u2032", "'")
        .replace("\uFF07", "'")
    )

    # Full width signs
    s = s.replace("\uFF20", "@").replace("\uFF0E", ".")

    return s


def preprocess_text(text: str) -> str:
    """Pre-process text before running e-mail extraction regexes."""

    text = normalize_text(text)

    atext = "A-Za-z0-9!#$%&'*+/=?^_`{|}~.-"
    # Glue hyphenated line breaks and plain line breaks inside addresses
    text = re.sub(fr"([{atext}])-\n([{atext}])", r"\1-\2", text)
    text = re.sub(fr"([{atext}])\n([{atext}])", r"\1\2", text)
    return text


def normalize_email(s: str) -> str:
    """Normalize an e-mail address for comparison/deduplication."""

    return normalize_text(s).strip().lower()
