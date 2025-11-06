"""Lightweight helpers for quick PDF scans using PyMuPDF."""

from __future__ import annotations

from pathlib import Path
from typing import Set

from emailbot.config import PDF_MAX_PAGES, PARSE_COLLECT_ALL
from emailbot.cancel_token import is_cancelled
from emailbot.ui.progress_state import ParseProgress


def extract_emails_fitz(
    pdf_path: Path,
    progress: ParseProgress | None = None,
) -> Set[str]:
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
            if is_cancelled():
                break
            if PDF_MAX_PAGES and index >= PDF_MAX_PAGES:
                break
            if progress:
                if index == 0:
                    try:
                        progress.set_total(getattr(doc, "page_count", 0))
                    except Exception:
                        pass
                progress.inc_pages(1)
            try:
                text = page.get_text("text") or ""
            except Exception:
                text = ""
            if text:
                found |= emails_from_text(text)
            if progress:
                progress.set_found(len(found))
            if not PARSE_COLLECT_ALL and len(found) >= 10:
                break
    finally:
        try:
            doc.close()
        except Exception:
            pass
    return found
