"""Helpers for safe logging in the aiogram entrypoint."""

from __future__ import annotations


def mask_email(addr: str) -> str:
    """Return an obfuscated representation of an e-mail for INFO logs."""

    if not addr:
        return "***"
    parts = addr.split("@", 1)
    if len(parts) != 2:
        return "***"
    local, domain = parts
    local = local.strip()
    if not local:
        return f"***@{domain}"
    if len(local) <= 2:
        prefix = local[:1]
        suffix = local[-1:]
    else:
        prefix = local[:1]
        suffix = local[-1:]
    return f"{prefix}***{suffix}@{domain}"
