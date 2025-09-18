from __future__ import annotations

"""Helpers for matching FIO strings with e-mail local parts."""

import re
from typing import List

RU = r"[А-ЯЁ][а-яё]+"
EN = r"[A-Z][a-z]+"
NAME_RE = re.compile(rf"((?:{RU})|(?:{EN}))(?:\s+((?:{RU})|(?:{EN})))(?:\s+((?:{RU})|(?:{EN})))?")


def extract_names(text: str) -> List[str]:
    """Extract candidate FIO fragments from free-form text."""

    names = set()
    for match in NAME_RE.finditer(text or ""):
        parts = [part for part in match.groups() if part]
        if len(parts) >= 2:
            names.add(" ".join(parts))
    return sorted(names)


def translit_basic(s: str) -> str:
    """Perform a lightweight Cyrillic → Latin transliteration."""

    table = str.maketrans(
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
    return (s or "").lower().translate(table)


def fio_match_score(local: str, fio: str) -> float:
    """Compute a rudimentary match score for FIO ↔ local part."""

    local_part = (local or "").lower()
    parts = fio.split()
    if len(parts) < 2:
        return 0.0
    first, last = parts[0], parts[1]
    first_tr = translit_basic(first.lower())
    last_tr = translit_basic(last.lower())
    score = 0.0
    first_initial = first_tr[:1]
    if f"{first_tr}.{last_tr}" in local_part or f"{first_tr}_{last_tr}" in local_part:
        score = max(score, 0.9)
    if first_initial and (
        f"{first_initial}.{last_tr}" in local_part or f"{first_initial}{last_tr}" in local_part
    ):
        score = max(score, 0.8)
    if last_tr in local_part and first_initial in local_part:
        score = max(score, 0.6)
    return round(score, 2)


__all__ = ["extract_names", "fio_match_score", "translit_basic"]
