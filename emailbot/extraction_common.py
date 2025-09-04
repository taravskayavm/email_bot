"""Common helpers for e-mail extraction and normalization."""
from __future__ import annotations

import base64
import binascii
import re
import unicodedata
from html import unescape

from .tld_registry import tld_of, is_known_tld

__all__ = [
    "normalize_text",
    "preprocess_text",
    "normalize_email",
    "maybe_decode_base64",
    "is_valid_domain",
    "filter_invalid_tld",
]

# Mapping of Cyrillic homoglyphs to their Latin counterparts
_CYR_TO_LATIN = str.maketrans({
    "а": "a",
    "е": "e",
    "о": "o",
    "р": "p",
    "с": "c",
    "х": "x",
    "А": "A",
    "Е": "E",
    "О": "O",
    "Р": "P",
    "С": "C",
    "Х": "X",
})

# Thin/zero-width spaces which should be treated as regular spaces
_Z_SPACE_RE = re.compile(r"[\u2000-\u200A\u202F\u205F\u3000]")


def normalize_text(s: str) -> str:
    """Return ``s`` normalized for e-mail extraction."""

    s = unicodedata.normalize("NFKC", s or "")
    s = unescape(s)
    s = s.translate(_CYR_TO_LATIN)

    # Spaces
    s = s.replace("\u00A0", " ")  # NBSP
    s = _Z_SPACE_RE.sub(" ", s)

    # Zero-width characters
    s = (
        s.replace("\u200B", "")
        .replace("\u200C", "")
        .replace("\u200D", "")
        .replace("\uFEFF", "")
    )

    # Various dashes/minuses -> ASCII '-'
    s = (
        s.replace("\u2010", "-")
        .replace("\u2011", "-")
        .replace("\u2012", "-")
        .replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2015", "-")
        .replace("\u2212", "-")
        .replace("\u2043", "-")
        .replace("\uFE63", "-")
        .replace("\uFF0D", "-")
    )

    # Apostrophes/quotes -> ASCII "'"
    s = (
        s.replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u2032", "'")
        .replace("\uFF07", "'")
    )

    # Full width signs
    s = s.replace("\uFF20", "@").replace("\uFF0E", ".")

    return s


_B64_ALLOWED = re.compile(r"^[A-Za-z0-9+/=\n\r\s]+$")


def maybe_decode_base64(s: str) -> str | None:
    """Return decoded base64 ``s`` if it looks like an e-mail container.

    ``s`` is decoded only if it is short enough and contains only base64
    characters.  If decoding fails or the result does not contain the ``@``
    character, ``None`` is returned.
    """

    if not s:
        return None
    s = s.strip()
    if len(s) < 8 or len(s) > 200:
        return None
    if not _B64_ALLOWED.match(s):
        return None
    try:
        decoded = base64.b64decode(s, validate=True)
    except (binascii.Error, ValueError):
        return None
    text = decoded.decode("utf-8", "ignore")
    if "@" not in text:
        return None
    return text


def preprocess_text(text: str) -> str:
    """Pre-process text before running e-mail extraction regexes."""

    text = normalize_text(text)

    # Glue hyphenated/soft hyphen line breaks inside addresses starting from
    # the second local-part character so that leading digits aren't lost.
    text = re.sub(r"(?<=\w\w)-?\s*\n(?=[\w.])", "", text)
    text = re.sub(r"(?<=\w\w)\u00AD(?=[\w.])", "", text)
    return text


def normalize_email(s: str) -> str:
    """Normalize an e-mail address for comparison and deduplication.

    The function performs a number of transformations so that semantically
    identical addresses map to the same representation:

    * The input is fully normalised with :func:`normalize_text` which removes
      zero‑width characters and canonicalises homoglyphs.
    * The domain part is converted to punycode (IDNA) and lower‑cased.
    * Gmail/Googlemail addresses are canonicalised by removing dots from the
      local part and stripping ``+tag`` suffixes.

    Parameters
    ----------
    s:
        Raw e‑mail address.

    Returns
    -------
    str
        Normalised address suitable for comparisons and lookups.
    """

    s = (normalize_text(s or "").strip().strip("'\""))
    if "@" not in s:
        return s.lower()
    local, domain = s.rsplit("@", 1)

    # Convert domain to ASCII using IDNA; fall back to best-effort ASCII.
    try:
        domain = domain.encode("idna").decode("ascii")
    except Exception:
        domain = domain.encode("ascii", "ignore").decode("ascii")
    domain = domain.lower()

    # Gmail canonicalisation: ignore dots and "+tag" in the local part.
    if domain in {"gmail.com", "googlemail.com"}:
        domain = "gmail.com"
        local = local.split("+", 1)[0].replace(".", "")

    return f"{local.lower()}@{domain}"


def is_valid_domain(domain: str) -> bool:
    """Return ``True`` if ``domain`` is syntactically valid and has a known TLD."""

    if not domain or len(domain) > 253:
        return False
    try:
        ascii_domain = domain.encode("idna").decode("ascii")
    except Exception:
        return False
    labels = ascii_domain.split(".")
    if len(labels) < 2:
        return False
    for label in labels:
        if not (1 <= len(label) <= 63):
            return False
        if label[0] == "-" or label[-1] == "-":
            return False
        if not re.fullmatch(r"[A-Za-z0-9-]+", label):
            return False
    tld = tld_of(ascii_domain)
    return tld is not None and is_known_tld(tld)


def filter_invalid_tld(emails: list[str]) -> tuple[list[str], dict]:
    """Filter out e-mails with unknown or malformed TLDs.

    Returns a tuple ``(valid, stats)`` where ``stats['invalid_tld']`` is the
    number of addresses removed.
    """

    valid: list[str] = []
    dropped = 0
    for e in emails:
        if "@" not in e:
            dropped += 1
            continue
        domain = e.split("@", 1)[1]
        if is_valid_domain(domain):
            valid.append(e)
        else:
            dropped += 1
    return valid, {"invalid_tld": dropped}
