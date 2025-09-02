"""Helpers for extracting e-mail hits from URLs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List


@dataclass
class EmailHit:
    """Single e-mail occurrence with some context information.

    Parameters
    ----------
    email:
        Normalised e-mail address.
    source_ref:
        Identifier of the source (file, url, page).
    origin:
        Extraction origin, e.g. ``mailto`` or ``obfuscation``.
    pre:
        Text preceding the hit (up to ~40 characters).
    post:
        Text following the hit (up to ~40 characters).
    """

    email: str
    source_ref: str
    origin: str
    pre: str
    post: str


# Patterns for popular "obfuscated" e-mail formats such as
# ``name [at] host [dot] com``.
_OBFUSCATION_PATTERNS = [
    r"([\w.+-]+)\s*\[at\]\s*([\w.-]+)\s*\[dot\]\s*([A-Za-z]{2,})",
    r"([\w.+-]+)\s*\(at\)\s*([\w.-]+)\s*\(dot\)\s*([A-Za-z]{2,})",
    r"([\w.+-]+)\s+at\s+([\w.-]+)\s+dot\s+([A-Za-z]{2,})",
    r"([\w.+-]+)\s+собака\s+([\w.-]+)\s+точка\s+([A-Za-z]{2,})",
]


def extract_obfuscated_hits(text: str, source_ref: str) -> List[EmailHit]:
    """Return all ``EmailHit`` objects found via obfuscation patterns."""

    hits: List[EmailHit] = []
    for pat in _OBFUSCATION_PATTERNS:
        for m in re.finditer(pat, text, flags=re.I):
            start, end = m.span()
            pre = text[max(0, start - 40) : start]
            post = text[end : end + 40]
            email = f"{m.group(1)}@{m.group(2)}.{m.group(3)}".lower()
            hits.append(
                EmailHit(email=email, source_ref=source_ref, origin="obfuscation", pre=pre, post=post)
            )
    return hits


__all__ = ["EmailHit", "extract_obfuscated_hits"]

