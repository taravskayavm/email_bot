"""Lightweight helpers for quick PDF scans using PyMuPDF."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Set

from emailbot.config import PDF_MAX_PAGES, PARSE_COLLECT_ALL  # Импортируем лимиты для быстрой обработки
from emailbot.cancel_token import is_cancelled
from emailbot.utils.text_preprocess import normalize_for_email  # Подключаем нормализацию текста под e-mail


def extract_emails_from_pdf_fast_core(
    doc,
    *,
    progress: Optional[object] = None,
) -> Set[str]:
    """Iterate over ``doc`` pages and collect e-mails with lightweight parsing."""

    from emailbot.parsing.extract_from_text import emails_from_text

    found: Set[str] = set()
    try:
        total = getattr(doc, "page_count", len(doc))
    except Exception:
        total = 0
    if progress:
        try:
            if total:
                progress.set_total(total)
        except Exception:
            pass
        try:
            progress.set_phase("PDF")
        except Exception:
            pass
        try:
            progress.set_found(len(found))
        except Exception:
            pass
    for index, page in enumerate(doc):
        if is_cancelled():
            break
        if PDF_MAX_PAGES and index >= PDF_MAX_PAGES:
            break
        try:
            text = page.get_text("text") or ""  # Забираем текст страницы, обрабатывая пустые значения
        except Exception:
            text = ""  # При ошибке чтения страницы считаем текст пустым
        text = normalize_for_email(text)  # Приводим текст к форме, удобной для поиска адресов
        before = len(found)
        if text:
            found |= emails_from_text(text)
        if progress:
            try:
                progress.inc_pages(1)
                if len(found) != before:
                    progress.set_found(len(found))
            except Exception:
                pass
        if not PARSE_COLLECT_ALL and len(found) >= 10:
            break
    return found


def extract_emails_fitz(
    pdf_path: Path,
    progress: Optional[object] = None,
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

    try:
        if progress:
            try:
                total = getattr(doc, "page_count", len(doc))
            except Exception:
                total = 0
            if total:
                try:
                    progress.set_total(total)
                except Exception:
                    pass
        found = extract_emails_from_pdf_fast_core(doc, progress=progress)
    finally:
        try:
            doc.close()
        except Exception:
            pass
    return found
