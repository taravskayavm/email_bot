"""Utilities for provider-specific e-mail canonicalisation."""

from __future__ import annotations

import re
import unicodedata
from email.utils import parseaddr
from typing import Tuple

import idna

GMAIL_HOSTS = {"gmail.com", "googlemail.com"}
YAHOO_HOSTS = {
    "yahoo.com",
    "yahoo.co.uk",
    "yahoo.co.jp",
    "ymail.com",
    "rocketmail.com",
}
OUTLOOK_HOSTS = {
    "outlook.com",
    "hotmail.com",
    "live.com",
    "msn.com",
    "outlook.co.uk",
}

_PLUS_TAG_RE = re.compile(r"\+[^@]+$")


def _split(email: str) -> Tuple[str, str]:
    """Return ``(local, domain)`` extracted from ``email``."""

    _, addr = parseaddr(email or "")
    addr = unicodedata.normalize("NFKC", (addr or "").strip().lower())
    if "@" not in addr:
        return addr, ""
    local, domain = addr.split("@", 1)
    try:
        domain_ascii = idna.encode(domain).decode("ascii")
    except Exception:
        domain_ascii = domain
    return local, domain_ascii


def canonicalize_email(
    email: str,
    *,
    gmail_dots: bool = True,
    gmail_plus: bool = True,
    other_plus: bool = True,
) -> str:
    """Return canonical representation suitable for deduplication.

    Provider-specific rules supported:
    * Gmail – optional removal of dots in the local part and ``+tag`` suffixes.
    * Yahoo/Outlook – optional removal of ``+tag`` suffixes.
    * Other domains – returned as-is with optional ``+tag`` stripping when
      ``other_plus`` is ``True``.
    """

    if not email:
        return ""
    local, domain = _split(email)
    if not domain:
        return local

    if domain in GMAIL_HOSTS:
        if gmail_plus:
            local = _PLUS_TAG_RE.sub("", local)
        if gmail_dots:
            local = local.replace(".", "")
        domain = "gmail.com"
    elif domain in YAHOO_HOSTS or domain in OUTLOOK_HOSTS:
        if other_plus:
            local = _PLUS_TAG_RE.sub("", local)
    else:
        if other_plus:
            local = _PLUS_TAG_RE.sub("", local)

    return unicodedata.normalize("NFKC", f"{local}@{domain}")


__all__ = ["canonicalize_email"]
