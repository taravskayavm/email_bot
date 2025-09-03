"""Helpers for extracting e-mail hits from URLs."""

from __future__ import annotations

import re
from typing import Dict, List, Optional

from .extraction import EmailHit, _valid_local, _valid_domain

_OBFUSCATED_RE = re.compile(
    r"(?P<local>[\w.+-]+)\s*(?P<at>@|\(at\)|\[at\]|at|собака)\s*(?P<domain>[\w-]+(?:\s*(?:\.|dot|\(dot\)|\[dot\]|точка)\s*[\w-]+)+)",
    re.I,
)

_DOT_SPLIT_RE = re.compile(r"\s*(?:\.|dot|\(dot\)|\[dot\]|точка)\s*", re.I)


def extract_obfuscated_hits(
    text: str, source_ref: str, stats: Optional[Dict[str, int]] = None
) -> List[EmailHit]:
    """Return all ``EmailHit`` objects found via obfuscation patterns."""

    hits: List[EmailHit] = []
    for m in _OBFUSCATED_RE.finditer(text):
        local = m.group("local")
        at_token = m.group("at")
        domain_raw = m.group("domain")
        parts = [p for p in _DOT_SPLIT_RE.split(domain_raw) if p]
        domain = ".".join(parts)
        email = f"{local}@{domain}".lower()
        if not (_valid_local(local) and _valid_domain(domain)):
            continue
        start, end = m.span()
        pre = text[max(0, start - 16) : start]
        post = text[end : end + 16]
        hits.append(
            EmailHit(email=email, source_ref=source_ref, origin="obfuscation", pre=pre, post=post)
        )
    return hits


__all__ = ["extract_obfuscated_hits"]

