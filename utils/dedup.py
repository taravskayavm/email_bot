from __future__ import annotations

import re

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


__all__ = ["canonical"]
