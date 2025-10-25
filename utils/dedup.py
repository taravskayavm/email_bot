from __future__ import annotations

import re
from typing import Iterable

_AT_RX = re.compile(r"@", re.ASCII)


def canonical(addr: str) -> str:
    """Return canonical form for deduplication purposes only."""

    s = (addr or "").strip().lower()
    if not s or "@" not in s:
        return s
    local, domain = _AT_RX.split(s, 1)
    try:
        domain_ascii = domain.encode("idna").decode("ascii")
    except Exception:
        domain_ascii = domain
    if domain_ascii in {"gmail.com", "googlemail.com"}:
        local = local.split("+", 1)[0].replace(".", "")
        domain_ascii = "gmail.com"
    return f"{local}@{domain_ascii}"


def unique_keep_order(items: Iterable[str]) -> list[str]:
    """Return unique items preserving the original order (case-sensitive)."""

    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


__all__ = ["canonical", "unique_keep_order"]
