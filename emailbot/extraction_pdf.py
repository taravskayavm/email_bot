"""PDF extraction helpers with optional layout and OCR features."""
from __future__ import annotations

import logging
import os
import statistics
import time
from pathlib import Path
from typing import Dict, List, Optional

try:  # pragma: no cover - ``regex`` may be unavailable in runtime
    import regex as re  # type: ignore

    _REGEX_HAS_TIMEOUT = True
except Exception:  # pragma: no cover - fallback to stdlib ``re``
    import re  # type: ignore

    _REGEX_HAS_TIMEOUT = False

# Детект доступности pdfminer.six (и других бэкендов по мере добавления)
try:  # pragma: no cover - доступность зависит от окружения
    import pdfminer  # type: ignore  # noqa: F401

    _PDFMINER_AVAILABLE = True
except Exception:  # pragma: no cover
    _PDFMINER_AVAILABLE = False

# Заглушка: когда появится OCR, заменить на фактическую проверку.
_OCR_AVAILABLE = False


def backend_status() -> Dict[str, bool]:
    """Return availability flags for PDF extraction backends."""

    return {
        "pdfminer": _PDFMINER_AVAILABLE,
        "ocr": _OCR_AVAILABLE,
    }

from emailbot import settings
from emailbot.settings_store import get
from .extraction_common import normalize_email, preprocess_text
from .run_control import should_stop

_SUP_DIGITS = str.maketrans({
    "0": "⁰",
    "1": "¹",
    "2": "²",
    "3": "³",
    "4": "⁴",
    "5": "⁵",
    "6": "⁶",
    "7": "⁷",
    "8": "⁸",
    "9": "⁹",
})

_OCR_PAGE_LIMIT = 10
_OCR_TIME_LIMIT = 30  # seconds
_PDF_TEXT_TRUNCATE_LIMIT = 2_000_000

logger = logging.getLogger(__name__)

_SOFT_HYPH = "\u00AD"

INVISIBLES = ["\xad", "\u200b", "\u200c", "\u200d", "\ufeff"]
SUPERSCRIPTS = "\u00B9\u00B2\u00B3" + "".join(chr(c) for c in range(0x2070, 0x207A))
BASIC_EMAIL = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"

# Быстрый детектор «обычных» e-mail без тяжёлой предобработки
_QUICK_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}")
# Порог, начиная с которого страницу считаем «простой» и не гоним через тяжёлый пайплайн
_PDF_FAST_MIN_HITS = int(os.getenv("PDF_FAST_MIN_HITS", "8"))
_PDF_FAST_TIMEOUT_MS = int(os.getenv("PDF_FAST_TIMEOUT_MS", "60"))


def _legacy_cleanup_text(text: str) -> str:
    """Старый мягкий клинап (оставляем на всякий случай как pre-step).
    Основная нормализация теперь всегда через preprocess_text()."""

    for ch in INVISIBLES:
        text = text.replace(ch, "")
    text = text.translate({ord(c): None for c in SUPERSCRIPTS})
    # Только безопасное склеивание переносов внутри слов; остальное сделает preprocess_text
    text = re.sub(r"([A-Za-z0-9])-\n([A-Za-z0-9])", r"\1\2", text)
    return text


def _join_hyphen_breaks(txt: str) -> str:
    """Remove soft hyphen artefacts and glue A-\nB sequences into AB."""

    if not txt:
        return txt
    txt = txt.replace(_SOFT_HYPH, "")
    return re.sub(
        r"([A-Za-zА-Яа-яЁё0-9])-(?:\r?\n|\r)\s*([A-Za-zА-Яа-яЁё0-9])",
        r"\1\2",
        txt,
    )


def _join_email_linebreaks(txt: str) -> str:
    """Glue line breaks around '.' and '@' inside e-mail addresses."""

    if not txt:
        return txt
    txt = re.sub(
        r"([A-Za-z0-9_+\-])\.\s*(?:\r?\n|\r)\s*([A-Za-z0-9_+\-])",
        r"\1.\2",
        txt,
    )
    txt = re.sub(
        r"([A-Za-z0-9._+\-])@\s*(?:\r?\n|\r)\s*([A-Za-z0-9.-])",
        r"\1@\2",
        txt,
    )
    txt = re.sub(
        r"([A-Za-z0-9-])\s*(?:\r?\n|\r)\s*\.",
        r"\1.",
        txt,
    )
    return txt


def _maybe_join_pdf_breaks(text: str, *, join_hyphen: bool, join_email: bool) -> str:
    if not text:
        return text or ""
    out = text
    if join_hyphen:
        out = _join_hyphen_breaks(out)
    if join_email:
        out = _join_email_linebreaks(out)
    return out


def _quick_email_matches(text: str) -> list[tuple[str, int, int]]:
    if not text:
        return []
    matches: list[tuple[str, int, int]] = []
    iterator = None
    if _REGEX_HAS_TIMEOUT:
        try:
            iterator = _QUICK_EMAIL_RE.finditer(
                text,
                overlapped=False,
                timeout=_PDF_FAST_TIMEOUT_MS / 1000.0,
            )
        except Exception:
            iterator = _QUICK_EMAIL_RE.finditer(text)
    else:
        iterator = _QUICK_EMAIL_RE.finditer(text)
    for match in iterator:
        matches.append((match.group(0), match.start(), match.end()))
    return matches


def _page_text_layout(page) -> str:
    """Return page text reconstructing layout and superscript digits."""

    data = page.get_text("dict")
    chars: List[tuple[str, float]] = []
    sizes: List[float] = []
    for block in data.get("blocks", []):
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                size = float(span.get("size", 0))
                text = span.get("text", "")
                for ch in text:
                    chars.append((ch, size))
                    sizes.append(size)
            chars.append(("\n", 0))
    if chars and chars[-1][0] == "\n":
        chars.pop()
    median = statistics.median(sizes) if sizes else 0
    out = []
    for ch, size in chars:
        if ch.isdigit() and median and size < median * 0.8:
            out.append(_SUP_DIGITS.get(ch, ch))
        else:
            out.append(ch)
    return "".join(out)


def _ocr_page(page) -> str:
    try:
        import pytesseract  # type: ignore
        from PIL import Image  # type: ignore
    except Exception:
        logger.warning(
            "pytesseract/Pillow are not installed; PDF OCR is disabled"
        )
        return ""
    try:
        pix = page.get_pixmap(dpi=200)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        return pytesseract.image_to_string(img)
    except Exception:
        return ""


def _fitz_extract(path: Path) -> str:
    try:
        import fitz  # type: ignore
    except Exception:
        logger.warning("PyMuPDF (fitz) is not installed; PDF text extraction disabled")
        return ""

    doc = None
    try:
        doc = fitz.open(str(path))
    except Exception:
        logger.warning("Failed to open PDF with PyMuPDF; falling back to other backends")
        return ""

    chunks: list[str] = []
    try:
        for page in doc:
            try:
                text = page.get_text() or ""
            except Exception:
                text = ""
            if text:
                chunks.append(text)
    finally:
        try:
            if doc is not None:
                doc.close()
        except Exception:
            pass
    return "\n".join(chunks)


def _extract_with_pypdf(path: Path) -> str:
    try:
        import pypdf
    except Exception:
        logger.warning("pypdf is not installed; PDF text extraction fallback disabled")
        return ""

    try:
        reader = pypdf.PdfReader(str(path))
    except Exception:
        return ""

    chunks: list[str] = []
    for page in getattr(reader, "pages", []) or []:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text:
            chunks.append(text)
    return "\n".join(chunks)


def _pdfminer_extract(path: Path) -> str:
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
    except Exception:
        logger.warning("pdfminer.six is not installed; PDF text extraction disabled")
        return ""

    try:
        return pdfminer_extract(str(path)) or ""
    except Exception:
        return ""


def cleanup_text(text: str) -> str:
    if not text:
        return ""
    text = _legacy_cleanup_text(text)
    return preprocess_text(text, stats=None)


def separate_around_emails(text: str) -> str:
    """Historical shim: preprocessing теперь делает нужные вставки пробелов."""

    return text


def extract_text_from_pdf(path: str | Path) -> str:
    pdf_path = Path(path)

    text = _fitz_extract(pdf_path)
    if not text or not text.strip():
        fallback = _extract_with_pypdf(pdf_path)
        text = fallback if fallback.strip() else ""
    if not text:
        text = _pdfminer_extract(pdf_path)
    if not text:
        return ""
    if len(text) > _PDF_TEXT_TRUNCATE_LIMIT:
        text = text[:_PDF_TEXT_TRUNCATE_LIMIT]
    return cleanup_text(text)


def extract_text(path: str) -> str:
    """Упрощённое извлечение текста для ``emailbot.extraction``."""

    pdf_path = Path(path)
    try:
        if _PDFMINER_AVAILABLE:
            text = _pdfminer_extract(pdf_path)
            if text:
                return text
    except Exception as exc:  # pragma: no cover - зависит от окружения
        logging.getLogger(__name__).warning("pdf extract failed for %s: %s", pdf_path, exc)
    return ""


def extract_from_pdf(path: str, stop_event: Optional[object] = None) -> tuple[list["EmailHit"], Dict]:
    """Extract e-mail addresses from a PDF file."""

    from .dedupe import merge_footnote_prefix_variants, repair_footnote_singletons
    from .extraction import EmailHit, extract_emails_document, _dedupe

    settings.load()
    strict = get("STRICT_OBFUSCATION", settings.STRICT_OBFUSCATION)
    radius = get("FOOTNOTE_RADIUS_PAGES", settings.FOOTNOTE_RADIUS_PAGES)
    layout = get("PDF_LAYOUT_AWARE", settings.PDF_LAYOUT_AWARE)
    ocr = get("ENABLE_OCR", settings.ENABLE_OCR)
    join_hyphen_breaks = get("PDF_JOIN_HYPHEN_BREAKS", True)
    join_email_breaks = get("PDF_JOIN_EMAIL_BREAKS", True)

    stats: Dict[str, int] = {"pages": 0}

    try:
        import fitz  # type: ignore
    except Exception:
        try:
            with open(path, "rb") as f:
                text = f.read().decode("utf-8", "ignore")
        except Exception:
            return [], {"errors": ["cannot open"]}
        text = _maybe_join_pdf_breaks(
            text,
            join_hyphen=join_hyphen_breaks,
            join_email=join_email_breaks,
        )
        # --- EB-PDF-043D: аварийный таймаут/обрезка текста ---
        # На случай зацикливания pdfminer или OCR-процесса. Ограничим длину
        # текста (например, 2 МБ), чтобы downstream-обработка не подвисала на
        # аномально больших буферах.
        if len(text) > _PDF_TEXT_TRUNCATE_LIMIT:
            text = text[:_PDF_TEXT_TRUNCATE_LIMIT]
            stats["pdf_text_truncated"] = stats.get("pdf_text_truncated", 0) + 1
        # Обработка текста из fallback ветки через единый preprocess_text
        hits = [
            EmailHit(email=e, source_ref=f"pdf:{path}", origin="direct_at")
            for e in extract_emails_document(text, stats)
        ]
        return _dedupe(hits), {"pages": 0, "needs_ocr": True}

    hits: List[EmailHit] = []
    doc = fitz.open(path)
    ocr_pages = 0
    ocr_start = time.time()
    for page_idx, page in enumerate(doc, start=1):
        if should_stop() or (
            stop_event and getattr(stop_event, "is_set", lambda: False)()
        ):
            break
        stats["pages"] += 1
        if layout:
            try:
                text = _page_text_layout(page)
            except Exception:
                text = page.get_text() or ""
        else:
            text = page.get_text() or ""
        if not text.strip() and ocr:
            if (
                ocr_pages < _OCR_PAGE_LIMIT
                and time.time() - ocr_start < _OCR_TIME_LIMIT
            ):
                text = _ocr_page(page)
                if text:
                    ocr_pages += 1
                    stats["ocr_pages"] = ocr_pages
        text = _maybe_join_pdf_breaks(
            text,
            join_hyphen=join_hyphen_breaks,
            join_email=join_email_breaks,
        )
        if len(text) > _PDF_TEXT_TRUNCATE_LIMIT:
            text = text[:_PDF_TEXT_TRUNCATE_LIMIT]
            stats["pdf_text_truncated"] = stats.get("pdf_text_truncated", 0) + 1

        quick_matches = _quick_email_matches(text)
        fast_norms: set[str] = set()
        if len(quick_matches) >= _PDF_FAST_MIN_HITS:
            fast_hits: list[EmailHit] = []
            for raw_email, start, end in quick_matches:
                norm = normalize_email(raw_email)
                if not norm or norm in fast_norms:
                    continue
                fast_norms.add(norm)
                pre = text[max(0, start - 16) : start]
                post = text[end : end + 16]
                fast_hits.append(
                    EmailHit(
                        email=raw_email,
                        source_ref=f"pdf:{path}#page={page_idx}",
                        origin="direct_at",
                        pre=pre,
                        post=post,
                    )
                )
            if fast_hits:
                hits.extend(fast_hits)
            stats["pdf_fast_pages"] = stats.get("pdf_fast_pages", 0) + 1
            stats["pdf_fast_hits"] = stats.get("pdf_fast_hits", 0) + len(fast_hits)

        text = _legacy_cleanup_text(text)
        text = preprocess_text(text, stats)
        low_text = text.lower()
        for email in extract_emails_document(text, stats):
            norm = normalize_email(email)
            if norm and norm in fast_norms:
                continue
            for m in re.finditer(re.escape(email), low_text):
                start, end = m.span()
                pre = text[max(0, start - 16) : start]
                post = text[end : end + 16]
                hits.append(
                    EmailHit(
                        email=email,
                        source_ref=f"pdf:{path}#page={page_idx}",
                        origin="direct_at",
                        pre=pre,
                        post=post,
                    )
                )
        if should_stop() or (
            stop_event and getattr(stop_event, "is_set", lambda: False)()
        ):
            break
    doc.close()
    if ocr:
        logger.debug("ocr_pages=%d", ocr_pages)

    hits = merge_footnote_prefix_variants(hits, stats)
    hits, fstats = repair_footnote_singletons(hits, layout)
    for k, v in fstats.items():
        if v:
            stats[k] = stats.get(k, 0) + v
    hits = _dedupe(hits)

    return hits, stats


def extract_from_pdf_stream(
    data: bytes, source_ref: str, stop_event: Optional[object] = None
) -> tuple[list["EmailHit"], Dict]:
    """Extract e-mail addresses from PDF bytes."""


    from .dedupe import merge_footnote_prefix_variants, repair_footnote_singletons
    from .extraction import EmailHit, extract_emails_document, _dedupe

    settings.load()
    strict = get("STRICT_OBFUSCATION", settings.STRICT_OBFUSCATION)
    radius = get("FOOTNOTE_RADIUS_PAGES", settings.FOOTNOTE_RADIUS_PAGES)
    layout = get("PDF_LAYOUT_AWARE", settings.PDF_LAYOUT_AWARE)
    ocr = get("ENABLE_OCR", settings.ENABLE_OCR)
    join_hyphen_breaks = get("PDF_JOIN_HYPHEN_BREAKS", True)
    join_email_breaks = get("PDF_JOIN_EMAIL_BREAKS", True)

    stats: Dict[str, int] = {"pages": 0}

    try:
        import fitz  # type: ignore
    except Exception:
        try:
            text = data.decode("utf-8", "ignore")
        except Exception:
            return [], {"errors": ["cannot open"]}
        text = _maybe_join_pdf_breaks(
            text,
            join_hyphen=join_hyphen_breaks,
            join_email=join_email_breaks,
        )
        if len(text) > _PDF_TEXT_TRUNCATE_LIMIT:
            text = text[:_PDF_TEXT_TRUNCATE_LIMIT]
            stats["pdf_text_truncated"] = stats.get("pdf_text_truncated", 0) + 1
        text = preprocess_text(text, stats=None)
        hits = [
            EmailHit(email=e, source_ref=source_ref, origin="direct_at")
            for e in extract_emails_document(text, stats)
        ]
        return _dedupe(hits), {"pages": 0, "needs_ocr": True}

    hits: List[EmailHit] = []
    doc = fitz.open(stream=data, filetype="pdf")
    ocr_pages = 0
    ocr_start = time.time()
    for page_idx, page in enumerate(doc, start=1):
        if should_stop() or (
            stop_event and getattr(stop_event, "is_set", lambda: False)()
        ):
            break
        stats["pages"] += 1
        if layout:
            try:
                text = _page_text_layout(page)
            except Exception:
                text = page.get_text() or ""
        else:
            text = page.get_text() or ""
        if not text.strip() and ocr:
            if (
                ocr_pages < _OCR_PAGE_LIMIT
                and time.time() - ocr_start < _OCR_TIME_LIMIT
            ):
                text = _ocr_page(page)
                if text:
                    ocr_pages += 1
                    stats["ocr_pages"] = ocr_pages
        text = _maybe_join_pdf_breaks(
            text,
            join_hyphen=join_hyphen_breaks,
            join_email=join_email_breaks,
        )
        if len(text) > _PDF_TEXT_TRUNCATE_LIMIT:
            text = text[:_PDF_TEXT_TRUNCATE_LIMIT]
            stats["pdf_text_truncated"] = stats.get("pdf_text_truncated", 0) + 1

        quick_matches = _quick_email_matches(text)
        fast_norms: set[str] = set()
        if len(quick_matches) >= _PDF_FAST_MIN_HITS:
            fast_hits: list[EmailHit] = []
            for raw_email, start, end in quick_matches:
                norm = normalize_email(raw_email)
                if not norm or norm in fast_norms:
                    continue
                fast_norms.add(norm)
                pre = text[max(0, start - 16) : start]
                post = text[end : end + 16]
                fast_hits.append(
                    EmailHit(
                        email=raw_email,
                        source_ref=f"{source_ref}#page={page_idx}",
                        origin="direct_at",
                        pre=pre,
                        post=post,
                    )
                )
            if fast_hits:
                hits.extend(fast_hits)
            stats["pdf_fast_pages"] = stats.get("pdf_fast_pages", 0) + 1
            stats["pdf_fast_hits"] = stats.get("pdf_fast_hits", 0) + len(fast_hits)

        text = _legacy_cleanup_text(text)
        text = preprocess_text(text, stats)
        low_text = text.lower()
        for email in extract_emails_document(text, stats):
            norm = normalize_email(email)
            if norm and norm in fast_norms:
                continue
            for m in re.finditer(re.escape(email), low_text):
                start, end = m.span()
                pre = text[max(0, start - 16) : start]
                post = text[end : end + 16]
                hits.append(
                    EmailHit(
                        email=email,
                        source_ref=f"{source_ref}#page={page_idx}",
                        origin="direct_at",
                        pre=pre,
                        post=post,
                    )
                )
        if should_stop() or (
            stop_event and getattr(stop_event, "is_set", lambda: False)()
        ):
            break
    doc.close()
    if ocr:
        logger.debug("ocr_pages=%d", ocr_pages)

    hits = merge_footnote_prefix_variants(hits, stats)
    hits, fstats = repair_footnote_singletons(hits, layout)
    for k, v in fstats.items():
        if v:
            stats[k] = stats.get(k, 0) + v
    hits = _dedupe(hits)

    return hits, stats


__all__ = [
    "INVISIBLES",
    "SUPERSCRIPTS",
    "BASIC_EMAIL",
    "cleanup_text",
    "separate_around_emails",
    "extract_text_from_pdf",
    "extract_text",
    "extract_from_pdf",
    "extract_from_pdf_stream",
]
