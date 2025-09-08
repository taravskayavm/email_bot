"""Utilities for manual e-mail sending.

This module currently exposes :func:`parse_manual_input` which is used to
normalize user supplied e-mail addresses.  The function extracts potential
addresses from arbitrary text, sanitises them and removes duplicates including
"footnote" variants ("55alex@example.com" vs ``alex@example.com``).
"""

from __future__ import annotations

from utils.email_clean import (
    extract_emails,
    sanitize_email,
    dedupe_with_variants,
)


def parse_manual_input(text: str) -> list[str]:
    """Extract e-mail addresses from arbitrary text.

    The parser supports mixed separators (commas, spaces, semicolons, new
    lines), strips surrounding punctuation, drops invalid addresses and
    de‑duplicates, removing "footnote" prefixes such as ``55alex@``.

    Parameters
    ----------
    text:
        Raw text entered by a user in the chat.

    Returns
    -------
    list[str]
        A list of cleaned and unique e‑mail addresses.
    """

    # 1) Extract raw e-mail substrings
    raw = extract_emails(text)
    if not raw:
        return []

    # 2) Sanitize each address; ``sanitize_email`` returns "" for invalid ones
    cleaned = [sanitize_email(e) for e in raw]
    cleaned = [e for e in cleaned if e]

    # 3) Deduplicate, removing footnote variants
    emails = dedupe_with_variants(cleaned)
    return emails


__all__ = ["parse_manual_input"]

