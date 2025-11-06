"""Utility helpers for lightweight OCR capability checks."""

from __future__ import annotations

import os
import shutil
from typing import Any

from emailbot.config import (
    PDF_OCR_MIN_CHARS,
    PDF_OCR_MIN_TEXT_RATIO,
    PDF_OCR_PROBE_PAGES,
    TESSERACT_CMD,
)


def ocr_available() -> bool:
    """Return ``True`` when pytesseract and the ``tesseract`` binary are available."""

    try:
        import pytesseract  # type: ignore  # noqa: F401
    except Exception:
        return False

    if TESSERACT_CMD:
        return os.path.exists(TESSERACT_CMD)
    return shutil.which("tesseract") is not None


def needs_ocr(doc: Any) -> bool:
    """Heuristically decide whether ``doc`` requires OCR fallback."""

    try:
        total_pages = len(doc)
    except Exception:
        return True

    if total_pages <= 0:
        return True

    probe = max(1, min(PDF_OCR_PROBE_PAGES, total_pages))
    text_pages = 0
    checked = 0

    for index in range(probe):
        try:
            page_text = doc[index].get_text("text") or ""
        except Exception:
            page_text = ""
        if len(page_text) >= PDF_OCR_MIN_CHARS:
            text_pages += 1
        checked += 1

    if checked == 0:
        return True

    return (text_pages / float(checked)) < PDF_OCR_MIN_TEXT_RATIO


__all__ = ["ocr_available", "needs_ocr"]
