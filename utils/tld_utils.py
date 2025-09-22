from __future__ import annotations

import os

import idna

_DEFAULT_ALLOWED = {"ru", "com"}


def _normalize_tld(value: str) -> str:
    raw = (value or "").strip().lstrip(".").lower()
    if not raw:
        return ""
    try:
        ascii_tld = idna.encode(raw).decode("ascii")
    except Exception:
        return raw
    return ascii_tld


def allowed_tlds() -> set[str]:
    """Return the set of allowed top-level domains."""

    env = os.getenv("TLD_ALLOWED", "")
    if env.strip():
        return {
            tld
            for part in env.split(",")
            if (tld := _normalize_tld(part))
        }
    return {_normalize_tld(tld) for tld in _DEFAULT_ALLOWED}


def get_allowed_tlds() -> set[str]:
    """Explicit accessor used by parsing helpers."""

    return allowed_tlds()


def is_allowed_domain(domain: str) -> bool:
    """Return ``True`` if the domain belongs to the allow-list."""

    d = (domain or "").strip().lower()
    if "." not in d:
        return False
    tld = _normalize_tld(d.rsplit(".", 1)[-1])
    if not tld:
        return False
    return tld in allowed_tlds()


def is_foreign_domain(domain: str) -> bool:
    """Return ``True`` for domains outside of the allow-list."""

    host = (domain or "").strip().lower()
    if "." not in host:
        return False
    try:
        tld = _normalize_tld(host.rsplit(".", 1)[-1])
    except Exception:
        return True
    if not tld:
        return True
    return tld not in allowed_tlds()


__all__ = ["allowed_tlds", "get_allowed_tlds", "is_allowed_domain", "is_foreign_domain"]
