"""Lightweight helpers for quick PDF scans using PyMuPDF."""

from __future__ import annotations

from pathlib import Path
from typing import Set

import emailbot.config as config


def extract_emails_fitz(pdf_path: Path) -> Set[str]:
    """Extract a handful of e-mails using PyMuPDF if available."""

    try:
        import fitz  # type: ignore
        from emailbot.parsing.extract_from_text import emails_from_text
    except Exception:
        return set()

    try:
        doc = fitz.open(str(pdf_path))
    except Exception:
        return set()

    found: Set[str] = set()
    try:
        for index, page in enumerate(doc):
            if index >= config.PDF_MAX_PAGES or len(found) >= 10:
                break
            try:
                text = page.get_text("text") or ""
            except Exception:
                text = ""
            if text:
                found |= emails_from_text(text)
            if len(found) >= 10:
                break
    finally:
        try:
            doc.close()
        except Exception:
            pass
    return found
