from __future__ import annotations

import os

import idna

IDNA_DOMAIN_NORMALIZE = os.getenv("IDNA_DOMAIN_NORMALIZE", "0") == "1"


def _to_idna(domain: str) -> str:
    try:
        return idna.encode(domain, uts46=True).decode("ascii")
    except Exception:
        return domain

_DEFAULT_ALLOWED = {"ru", "com"}


def _normalize_tld(value: str) -> str:
    raw = (value or "").strip().lstrip(".").lower()
    if not raw:
        return ""
    if IDNA_DOMAIN_NORMALIZE:
        ascii_tld = _to_idna(raw)
        if ascii_tld:
            return ascii_tld.lower()
    try:
        raw.encode("ascii")
    except UnicodeEncodeError:
        return raw
    return raw


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
    if IDNA_DOMAIN_NORMALIZE and d:
        d = _to_idna(d).lower()
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
