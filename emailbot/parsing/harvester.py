"""Aggressive e-mail harvesting helpers with obfuscation support."""

from __future__ import annotations

import re
import unicodedata

__all__ = ["harvest_emails"]

ZWS = "".join(chr(c) for c in (0x200B, 0x200C, 0x200D, 0x2060, 0xFEFF))

# Базовая «строгая» маска
STRICT_EMAIL = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")

# Разрешаем точки разных юникод-вариантов и обфускации (at/dot/«собака/точка»)
OBFUSCATED = re.compile(
    r"""
  (?P<loc>[A-Za-z0-9._%+\-]{1,64}(?:\s*[.\u00B7\u2219\u30FB\ufe52\-]\s*[A-Za-z0-9._%+\-]{1,64})*)
  \s*(?:@|\(at\)|\[at\]|{at}| at | собака )\s*
  (?P<dom>[A-Za-z0-9\-]{1,63}(?:\s*[.\u00B7\u2219\u30FB\ufe52]\s*[A-Za-z0-9\-]{1,63})+)
""",
    re.IGNORECASE | re.VERBOSE,
)

DOTS = ("(dot)", "[dot]", "{dot}", " dot ", " точка ", "·", "∙", "•", "⸱", "．", "。")


def _scrub(text: str) -> str:
    """Normalise ``text`` before e-mail extraction."""

    if not text:
        return ""
    cleaned = text.replace("\xa0", " ")
    cleaned = cleaned.translate({ord(c): None for c in ZWS})
    cleaned = unicodedata.normalize("NFKC", cleaned)
    # Склейка переносов внутри «email-подобных» последовательностей
    cleaned = re.sub(r"(?<=\w)[\r\n]{1,2}(?=[\w@.])", "", cleaned)
    return cleaned


def harvest_emails(raw_text: str) -> set[str]:
    """Extract e-mail addresses from ``raw_text`` using aggressive heuristics."""

    text = _scrub(raw_text)
    found: set[str] = {match.group(0) for match in STRICT_EMAIL.finditer(text)}
    for match in OBFUSCATED.finditer(text):
        loc = match.group("loc")
        dom = match.group("dom")
        if not loc or not dom:
            continue
        email = f"{loc}@{dom}"
        for dot in DOTS:
            email = email.replace(dot, ".")
        email = (
            email.replace("(at)", "@")
            .replace("[at]", "@")
            .replace("{at}", "@")
            .replace(" at ", "@")
            .replace(" собака ", "@")
        )
        email = re.sub(r"\s+", "", email)
        if "@" not in email:
            continue
        _, domain = email.split("@", 1)
        if "." not in domain:
            continue
        found.add(email)
    return found
