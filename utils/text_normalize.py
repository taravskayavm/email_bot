from __future__ import annotations

import re
import unicodedata

# Невидимые/служебные символы, часто попадающие из PDF/OCR и ломают парсинг
_ZERO_WIDTH = re.compile(r"[\u200B-\u200D\uFEFF]")

# Самые частые конфузаблы между кириллицей и латиницей
_CONFUSABLE = str.maketrans(
    {
        "А": "A",
        "В": "B",
        "Е": "E",
        "К": "K",
        "М": "M",
        "Н": "H",
        "О": "O",
        "Р": "P",
        "С": "C",
        "Т": "T",
        "У": "Y",
        "Х": "X",
        "а": "a",
        "е": "e",
        "о": "o",
        "р": "p",
        "с": "c",
        "у": "y",
        "х": "x",
    }
)


def normalize_text(src: str) -> str:
    """Normalize incoming text (NFKC + zero-width removal + confusables)."""

    if not src:
        return src
    normalized = unicodedata.normalize("NFKC", src)
    normalized = _ZERO_WIDTH.sub("", normalized)
    normalized = normalized.translate(_CONFUSABLE)
    return normalized


__all__ = ["normalize_text"]
