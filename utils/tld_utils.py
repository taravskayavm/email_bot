from __future__ import annotations

import os

_DEFAULT_ALLOWED = {"ru", "com"}


def allowed_tlds() -> set[str]:
    """Return the set of allowed top-level domains."""

    env = os.getenv("TLD_ALLOWED", "")
    if env.strip():
        return {
            t.strip().lstrip(".").lower()
            for t in env.split(",")
            if t.strip()
        }
    return set(_DEFAULT_ALLOWED)


def is_allowed_domain(domain: str) -> bool:
    """Return ``True`` if the domain belongs to the allow-list."""

    d = (domain or "").strip().lower()
    if "." not in d:
        return False
    tld = d.rsplit(".", 1)[-1]
    return tld in allowed_tlds()


def is_foreign_domain(domain: str) -> bool:
    """Return ``True`` for domains outside of the allow-list."""

    return not is_allowed_domain(domain)


__all__ = ["allowed_tlds", "is_allowed_domain", "is_foreign_domain"]
