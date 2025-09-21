from __future__ import annotations


def canonical(addr: str) -> str:
    """Return canonical form of an address for deduplication."""

    normalized = addr.strip().lower()
    if "@" not in normalized:
        return normalized
    local, domain = normalized.split("@", 1)
    if domain in {"gmail.com", "googlemail.com"}:
        local = local.split("+", 1)[0].replace(".", "")
        domain = "gmail.com"
    return f"{local}@{domain}"


__all__ = ["canonical"]
