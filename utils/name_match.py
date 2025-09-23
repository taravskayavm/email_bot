from __future__ import annotations

"""Helpers for matching FIO strings with e-mail local parts."""

import re
import unicodedata as ud
from typing import Iterable, List, Sequence, Tuple

NAME_WORD = r"[A-ZА-ЯЁ][A-Za-zА-ЯЁа-яё']*(?:-[A-Za-zА-ЯЁа-яё']+)*"
NAME_RE = re.compile(
    rf"\b({NAME_WORD})(?:\s+|,\s*)({NAME_WORD})(?:\s+({NAME_WORD}))?",
    re.UNICODE,
)
SURNAME_INITIAL_RE = re.compile(
    rf"\b(?P<last>{NAME_WORD})\s+(?P<first_i>[A-ZА-ЯЁ])\.\s*(?:(?P<middle_i>[A-ZА-ЯЁ])\.?)*",
    re.UNICODE,
)
INITIAL_SURNAME_RE = re.compile(
    rf"\b(?P<first_i>[A-ZА-ЯЁ])\.\s+(?P<last>{NAME_WORD})",
    re.UNICODE,
)

_HYPHEN_SURNAME_TOKEN_RE = re.compile(
    r"^[A-Za-zА-Яа-яЁё]+(?:-[A-Za-zА-Яа-яЁё]+)+$"
)
_INITIAL_SURNAME_TOKEN_RE = re.compile(
    r"^[A-Za-zА-Яа-яЁё]\.[A-Za-zА-Яа-яЁё]+(?:-[A-Za-zА-Яа-яЁё]+)*$"
)
_SURNAME_INITIAL_TOKEN_RE = re.compile(
    r"^[A-Za-zА-Яа-яЁё]+(?:-[A-Za-zА-Яа-яЁё]+)*\.[A-Za-zА-Яа-яЁё]$"
)

_TRANSLIT_TABLE = str.maketrans(
    {
        "а": "a",
        "б": "b",
        "в": "v",
        "г": "g",
        "д": "d",
        "е": "e",
        "ё": "e",
        "ж": "zh",
        "з": "z",
        "и": "i",
        "й": "y",
        "к": "k",
        "л": "l",
        "м": "m",
        "н": "n",
        "о": "o",
        "п": "p",
        "р": "r",
        "с": "s",
        "т": "t",
        "у": "u",
        "ф": "f",
        "х": "h",
        "ц": "c",
        "ч": "ch",
        "ш": "sh",
        "щ": "sch",
        "ы": "y",
        "э": "e",
        "ю": "yu",
        "я": "ya",
    }
)


def _strip_diacritics(text: str) -> str:
    """Remove combining marks while preserving base characters."""

    normalized = ud.normalize("NFKD", text or "")
    return "".join(ch for ch in normalized if not ud.combining(ch))


def translit_basic(s: str) -> str:
    """Perform a lightweight Cyrillic → Latin transliteration."""

    lowered = (s or "").lower()
    transliterated = lowered.translate(_TRANSLIT_TABLE)
    return _strip_diacritics(transliterated)


def _slug(text: str) -> str:
    """Lowercase, transliterate and strip punctuation from ``text``."""

    transliterated = translit_basic(text)
    return "".join(
        ch for ch in transliterated if ud.category(ch)[0] != "P" and not ch.isspace()
    )


def extract_names(text: str) -> List[str]:
    """Extract candidate FIO fragments from free-form text."""

    names = set()
    for match in NAME_RE.finditer(text or ""):
        parts = [part for part in match.groups() if part]
        if len(parts) >= 2:
            names.add(" ".join(parts))
    return sorted(names)


def fio_candidates(text: str) -> List[Tuple[str, str]]:
    """Return slugified (first, last) name pairs detected in ``text``."""

    seen: set[Tuple[str, str]] = set()
    raw = text or ""
    hints: list[Tuple[str, str]] = []

    for match in NAME_RE.finditer(raw):
        parts = [part for part in match.groups() if part]
        if len(parts) < 2:
            continue
        slugs = [_slug(part) for part in parts]
        for first, second in zip(slugs, slugs[1:]):
            if first and second:
                seen.add((first, second))
                seen.add((second, first))
        if len(slugs) == 3:
            first, third = slugs[0], slugs[2]
            if first and third:
                seen.add((first, third))
                seen.add((third, first))

    for regex in (SURNAME_INITIAL_RE, INITIAL_SURNAME_RE):
        for match in regex.finditer(raw):
            first_initial = _slug(match.group("first_i") or "")
            last = _slug(match.group("last") or "")
            if first_initial and last:
                seen.add((first_initial, last))
                seen.add((last, first_initial))

    for token in re.findall(r"[A-Za-zА-Яа-яЁё.\-]+", raw):
        candidate = token.strip()
        if not candidate:
            continue
        if _HYPHEN_SURNAME_TOKEN_RE.match(candidate):
            hints.append(("hyphen-fam", candidate))
            slug_token = _slug(candidate)
            parts = [part for part in candidate.split("-") if part]
            slug_parts = [_slug(part) for part in parts if part]
            for slug_part in slug_parts:
                if slug_token and slug_part:
                    seen.add((slug_part, slug_token))
                    seen.add((slug_token, slug_part))
            if len(slug_parts) >= 2:
                for left in slug_parts:
                    for right in slug_parts:
                        if left and right and left != right:
                            seen.add((left, right))
        elif _INITIAL_SURNAME_TOKEN_RE.match(candidate):
            hints.append(("initials", candidate))
            try:
                initial_part, surname_part = candidate.split(".", 1)
            except ValueError:
                continue
            first_slug = _slug(initial_part[:1])
            last_slug = _slug(surname_part)
            if first_slug and last_slug:
                seen.add((first_slug, last_slug))
                seen.add((last_slug, first_slug))
        elif _SURNAME_INITIAL_TOKEN_RE.match(candidate):
            hints.append(("initials", candidate))
            try:
                surname_part, initial_part = candidate.rsplit(".", 1)
            except ValueError:
                continue
            surname_slug = _slug(surname_part)
            initial_slug = _slug(initial_part[:1])
            if surname_slug and initial_slug:
                seen.add((surname_slug, initial_slug))
                seen.add((initial_slug, surname_slug))

    fio_candidates.last_hints = hints  # type: ignore[attr-defined]

    return sorted(seen)


_SEPARATORS: Tuple[str, ...] = (".", "_", "-", "")


def _patterns_for_pair(first: str, last: str) -> Sequence[Tuple[str, float]]:
    """Yield candidate substrings for matching ``first``/``last`` against locals."""

    if not last:
        return ()

    first = first or ""
    last = last or ""
    variants: set[Tuple[str, float]] = set()

    if first:
        first_is_initial = len(first) == 1
        if first_is_initial:
            for sep in _SEPARATORS:
                variants.add((f"{first}{sep}{last}", 0.9))
                variants.add((f"{last}{sep}{first}", 0.88))
        else:
            for sep in _SEPARATORS:
                variants.add((f"{first}{sep}{last}", 1.0))
                variants.add((f"{last}{sep}{first}", 0.97))
            initial = first[0]
            if initial:
                for sep in _SEPARATORS:
                    variants.add((f"{initial}{sep}{last}", 0.92))
                    variants.add((f"{last}{sep}{initial}", 0.9))

    return tuple((pattern, score) for pattern, score in variants if pattern)


def fio_match_score(
    local: str, text: str, *, candidates: Iterable[Tuple[str, str]] | None = None
) -> float:
    """Compute a rudimentary match score for FIO ↔ local part."""

    local_raw = (local or "").lower()
    if not local_raw:
        return 0.0

    local_slug = _slug(local_raw)
    pairs = list(candidates if candidates is not None else fio_candidates(text))
    if not pairs:
        return 0.0

    best = 0.0
    for first, last in pairs:
        if not first or not last:
            continue
        for pattern, score in _patterns_for_pair(first, last):
            if not pattern:
                continue
            if pattern in local_raw or pattern in local_slug:
                if score > best:
                    best = score
                if best >= 0.99:
                    return 1.0
    return round(best, 3)


__all__ = ["extract_names", "fio_candidates", "fio_match_score", "translit_basic"]
