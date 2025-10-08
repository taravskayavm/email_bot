"""Common helpers for e-mail extraction and normalization."""
from __future__ import annotations

import base64
import binascii
import re
import unicodedata
from datetime import datetime
from html import unescape
from typing import Any, Iterable

import idna

from .tld_registry import tld_of, is_known_tld
from .footnotes import remove_footnotes_safe
from .text_normalize import normalize_text_for_emails

__all__ = [
    "normalize_text",
    "preprocess_text",
    "normalize_domain",
    "normalize_email",
    "maybe_decode_base64",
    "is_valid_domain",
    "filter_invalid_tld",
    "score_candidate",
    "CANDIDATE_SCORE_THRESHOLD",
    "wrap_as_entries",
]

try:  # pragma: no cover - fallback is best-effort for optional dependency
    from .models import EmailEntry
except Exception:  # pragma: no cover
    EmailEntry = None  # type: ignore[assignment]

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

    # Zero-width / invisibles / BiDi marks / soft hyphen и пр.
    # Это центрально важно, чтобы не «съедалась» первая буква e-mail.
    INVISIBLES_RE = re.compile(
        r"[\u00AD"                # SOFT HYPHEN
        r"\u200B-\u200F"          # ZWSP..RLM
        r"\u202A-\u202E"          # LRE..RLO/PDF
        r"\u2028\u2029"           # LINE/PARAGRAPH SEP
        r"\u202F"                 # NARROW NBSP
        r"\u205F"                 # MEDIUM MATH SPACE
        r"\u2060-\u206F"          # WORD JOINER..INVISIBLE OPS
        r"\u2066-\u2069"          # LRI/RLI/FSI/PDI
        r"\uFEFF"                 # ZW NBSP (BOM)
        r"\u1680"                 # OGHAM SPACE MARK
        r"\u180E"                 # MONGOLIAN VOWEL SEPARATOR
        r"]"
    )
    s = INVISIBLES_RE.sub("", s)

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
_GLUE_JOIN_RE = re.compile(r"[а-яА-Яa-zA-Z0-9]\s*@\s*[а-яА-Яa-zA-Z0-9]")
EMAIL_LIKE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def _canonicalize_address(address: str) -> str:
    """Return ``address`` with normalised domain and Gmail rules applied."""

    local, domain = address.rsplit("@", 1)
    domain = normalize_domain(domain)
    if domain in {"gmail.com", "googlemail.com"}:
        domain = "gmail.com"
        local = local.split("+", 1)[0].replace(".", "")
    return f"{local.lower()}@{domain}"


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


def preprocess_text(text: str, stats: dict | None = None) -> str:
    """Pre-process text before running e-mail extraction regexes.

    ``stats['left_guard_skips']`` counts situations where a potential
    hyphenated line break was detected immediately after the first character of
    the local part and therefore was *not* glued to avoid losing that character.
    """

    raw_input = text or ""
    text = remove_footnotes_safe(raw_input)
    text = normalize_text(text)

    # --- EB-PARSE-GLUE-014D: разлепление «слово+email» прямо в общем пайплайне ---
    # Вставляем пробел ПЕРЕД e-mail, если слева «прилип» символ (буква/цифра/закрывающий знак/кавычка/двоеточие/равно).
    # Покрывает кейсы: "Россияivanov@...", ")ivanov@...", "E-mail:ivanov@..."
    # Count occurrences where the guard prevented removal
    if stats is not None:
        m1 = re.findall(r"(?<=\w)-?\s*\n(?=[\w.])", text)
        m2 = re.findall(r"(?<=\w\w)-?\s*\n(?=[\w.])", text)
        m3 = re.findall(r"(?<=\w)\u00AD(?=[\w.])", text)
        m4 = re.findall(r"(?<=\w\w)\u00AD(?=[\w.])", text)
        skips = len(m1) - len(m2) + len(m3) - len(m4)
        if skips:
            stats["left_guard_skips"] = stats.get("left_guard_skips", 0) + skips

    # Glue hyphenated/soft hyphen line breaks inside addresses starting from
    # the second local-part character so that leading digits aren't lost.
    text = re.sub(r"(?<=\w\w)-?\s*\n(?=[\w.])", "", text)
    text = re.sub(r"(?<=\w\w)\u00AD(?=[\w.])", "", text)

    EMAIL_TOKEN = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"
    before_email = re.compile(
        r"(?<=[A-Za-zА-Яа-яЁё0-9\)\]\u00BB\u201D'\":=])(?=" + EMAIL_TOKEN + r")"
    )
    current_text = text
    inserted_count = 0

    def _insert_left_space(match: re.Match[str]) -> str:
        nonlocal inserted_count
        pos = match.start()
        if pos > 0 and EMAIL_LIKE.match(current_text[pos - 1 :]):
            return ""
        inserted_count += 1
        return " "

    text = before_email.sub(_insert_left_space, current_text)
    if inserted_count and stats is not None:
        stats["email_left_glue_fixed"] = stats.get("email_left_glue_fixed", 0) + inserted_count

    text = normalize_text_for_emails(text)

    if _GLUE_JOIN_RE.search(raw_input):
        return f"{text} [[JOINED_BY_GLUE]]"
    return text


_PHONE_PREFIX_RE = re.compile(r"^(?:\+?\d[\d\- ]{6,}\d)(?=[A-Za-z])")


def strip_phone_prefix(local: str, stats: dict | None = None) -> tuple[str, bool]:
    """Remove a long phone number prefix stuck to the left of ``local``.

    Returns a tuple ``(new_local, changed)``. ``stats['phone_prefix_stripped']``
    is incremented when a prefix is removed.
    """

    m = _PHONE_PREFIX_RE.match(local)
    if m:
        if stats is not None:
            stats["phone_prefix_stripped"] = stats.get("phone_prefix_stripped", 0) + 1
        return local[m.end() :], True
    return local, False


def normalize_domain(domain: str) -> str:
    """Return ``domain`` normalised for comparison and validation."""

    raw = unicodedata.normalize("NFKC", (domain or "")).strip().strip(".")
    if not raw:
        return ""
    folded = raw.casefold()
    try:
        ascii_domain = idna.encode(folded, uts46=True).decode("ascii")
    except idna.IDNAError:
        try:
            ascii_domain = folded.encode("ascii", "ignore").decode("ascii")
        except Exception:
            ascii_domain = folded
    return ascii_domain.lower()


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
    match = EMAIL_LIKE.fullmatch(s)
    if match:
        return _canonicalize_address(match.group(0))

    s = re.sub(
        r"(?i)(?:(?<=^)|(?<=\s))[abc]\)\s*(?=[A-Za-z0-9._%+-]+@)",
        "",
        s,
    )
    match = EMAIL_LIKE.search(s)
    if match:
        return _canonicalize_address(match.group(0))

    if "@" not in s:
        return s.lower()

    return _canonicalize_address(s)


def is_valid_domain(domain: str) -> bool:
    """Return ``True`` if ``domain`` is syntactically valid and has a known TLD."""

    ascii_domain = normalize_domain(domain)
    if not ascii_domain or len(ascii_domain) > 253:
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


def score_candidate(features: dict) -> int:
    """Compute a simple score for a candidate e-mail address.

    The score is a sum of feature weights.  Currently only a handful of
    lightweight heuristics is implemented which allows the caller to decide
    whether the candidate should be accepted or placed into quarantine.

    Parameters
    ----------
    features:
        Mapping describing properties of the candidate.  Supported keys:

        ``tld_known`` (bool)
            ``True`` if the domain has a known TLD.

    Returns
    -------
    int
        Calculated score.
    """

    score = 0
    if features.get("tld_known"):
        score += 1
    return score


# Minimal score required for a candidate to be accepted.  Lower-scored
# addresses are put into a quarantine bucket and are not returned by the
# extractor.
CANDIDATE_SCORE_THRESHOLD = 1


def wrap_as_entries(
    emails: Iterable[str],
    *,
    source: str,
    status: str = "new",
    last_sent: datetime | None = None,
    meta: dict[str, Any] | None = None,
) -> list[EmailEntry] | list[str]:
    """Return ``EmailEntry`` objects for ``emails`` when the model is available.

    The helper keeps the rest of the codebase decoupled from the concrete data
    model: if :mod:`emailbot.models` cannot be imported, a plain list of strings
    is returned instead.  This enables a gradual migration towards the unified
    model without breaking existing call sites.
    """

    items = list(emails)
    if EmailEntry is None:  # pragma: no cover - defensive branch
        return items
    return EmailEntry.wrap_list(
        items,
        source=source,
        status=status,
        last_sent=last_sent,
        meta=meta,
    )
