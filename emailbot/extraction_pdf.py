"""PDF extraction helpers with optional layout and OCR features."""
from __future__ import annotations

import io
import logging
import os
import shutil
import statistics
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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

# Опциональный backend PyMuPDF (fitz)
try:  # pragma: no cover - PyMuPDF может отсутствовать в среде
    import fitz  # type: ignore

    FITZ_OK = True
except Exception:  # pragma: no cover - тихая деградация до pdfminer
    fitz = None  # type: ignore
    FITZ_OK = False

# Заглушка: когда появится OCR, заменить на фактическую проверку.
_OCR_AVAILABLE = False
_OCR_LOGGED_MISSING = False


def _detect_ocr_status() -> tuple[bool, bool, str]:
    """Determine whether OCR is enabled and available."""

    global _OCR_AVAILABLE, _OCR_LOGGED_MISSING
    enabled = bool(get("ENABLE_OCR", settings.ENABLE_OCR))
    if not enabled:
        _OCR_AVAILABLE = False
        return False, False, ""
    engine = shutil.which("tesseract")
    if engine:
        _OCR_AVAILABLE = True
        return True, True, ""
    _OCR_AVAILABLE = False
    if not _OCR_LOGGED_MISSING:
        logger.warning("OCR engine not found")
        _OCR_LOGGED_MISSING = True
    return False, True, "не найден tesseract"


def backend_status() -> Dict[str, bool | str]:
    """Return availability flags for PDF extraction backends."""

    ocr_available, ocr_enabled, reason = _detect_ocr_status()
    status: Dict[str, bool | str] = {
        "fitz": FITZ_OK,
        "pdfminer": _PDFMINER_AVAILABLE,
        "ocr": ocr_available if ocr_enabled else False,
        "ocr_enabled": ocr_enabled,
    }
    if reason:
        status["ocr_reason"] = reason
    return status

from emailbot import settings
from emailbot.settings_store import get
from .extraction_common import normalize_email, preprocess_text
from .run_control import should_stop
from .progress_watchdog import heartbeat_now
from emailbot.timebudget import TimeBudget

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

_OCR_PAGE_LIMIT = int(os.getenv("OCR_PAGE_LIMIT", "10"))
_OCR_TIME_LIMIT = int(os.getenv("OCR_TIME_LIMIT", "30"))  # seconds
_PDF_TEXT_TRUNCATE_LIMIT = int(os.getenv("PDF_TEXT_TRUNCATE_LIMIT", "2000000"))
MAX_PAGES = int(os.getenv("PDF_MAX_PAGES", "200"))

LEGACY_MODE = os.getenv("LEGACY_MODE", "0") == "1"
_pdf_backend_env = (os.getenv("PDF_BACKEND", "fitz") or "fitz").strip().lower()
if _pdf_backend_env not in {"fitz", "pdfminer", "auto"}:
    _pdf_backend_env = "fitz"
PDF_BACKEND = _pdf_backend_env

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
    # Уже есть базовая склейка в preprocess_text(), но для PDF полезно добить
    # частые артефакты, встречающиеся в выгрузках/конвертациях:
    # 1) пробелы вокруг '@'
    txt = re.sub(r"(\S)\s*@\s*(\S)", r"\1@\2", txt)
    # 2) переносы строки в доменной части: "name@\nmail.ru"
    txt = re.sub(r"@\s*(?:\r?\n|\r)\s*", "@", txt)
    # 3) невидимые символы прямо вокруг '@' (ZWSP и т.п.)
    txt = re.sub(r"@\u200B+", "@", txt)
    # 4) дефис в "e-mail" мешает склейке — нормализуем
    txt = txt.replace("e-mail", "email").replace("E-mail", "Email")
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


def _collect_fitz_text(doc, budget: TimeBudget | None = None) -> Tuple[str, int]:
    """Return concatenated text and a count of pages with non-empty content."""

    out: list[str] = []
    pages_with_text = 0
    for i, page in enumerate(doc):
        heartbeat_now()
        if budget:
            budget.checkpoint()
        if i >= MAX_PAGES:
            break
        try:
            text = page.get_text("text")
        except Exception:
            try:
                text = page.get_text()
            except Exception:
                text = ""
        if text and text.strip():
            pages_with_text += 1
            out.append(text)
    return "\n".join(out), pages_with_text


def _fitz_extract_with_stats(path: Path | str, budget: TimeBudget | None = None) -> Tuple[str, int]:
    if not FITZ_OK or fitz is None:
        return "", 0
    doc = None
    try:
        doc = fitz.open(str(path))
    except Exception:
        logger.warning("Failed to open PDF with PyMuPDF; falling back to other backends")
        return "", 0

    try:
        return _collect_fitz_text(doc, budget)
    finally:
        try:
            doc.close()
        except Exception:
            pass


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
    text, _ = _fitz_extract_with_stats(path)
    return text


def _pdfminer_extract(path: Path) -> str:
    text, _ = _pdfminer_extract_with_stats(path)
    return text


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


def _pdfminer_extract_with_stats(
    path: Path | str, budget: TimeBudget | None = None
) -> Tuple[str, int]:
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
    except Exception:
        logger.warning("pdfminer.six is not installed; PDF text extraction disabled")
        return "", 0

    if budget:
        budget.checkpoint()

    try:
        text = pdfminer_extract(str(path)) or ""
    except Exception:
        text = ""
    pages_with_text = 1 if text and text.strip() else 0
    return text, pages_with_text


def _pdfminer_extract_bytes_with_stats(
    data: bytes, budget: TimeBudget | None = None
) -> Tuple[str, int]:
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
    except Exception:
        logger.warning("pdfminer.six is not installed; PDF text extraction disabled")
        return "", 0

    if budget:
        budget.checkpoint()

    try:
        text = pdfminer_extract(io.BytesIO(data)) or ""
    except Exception:
        text = ""
    pages_with_text = 1 if text and text.strip() else 0
    return text, pages_with_text


def _backend_order() -> tuple[str, ...]:
    backend = PDF_BACKEND
    if LEGACY_MODE and backend != "pdfminer":
        backend = "fitz"
    if backend == "auto":
        return ("fitz", "pdfminer") if FITZ_OK else ("pdfminer",)
    if backend == "pdfminer":
        return ("pdfminer",)
    if backend == "fitz":
        return ("fitz",) if FITZ_OK else ("pdfminer",)
    return ("fitz", "pdfminer") if FITZ_OK else ("pdfminer",)


def _extract_with_backend(path: Path, backend: str) -> str:
    if backend == "fitz":
        return _fitz_extract(path)
    if backend == "pdfminer":
        if not _PDFMINER_AVAILABLE:
            return ""
        try:
            return _pdfminer_extract(path)
        except Exception as exc:  # pragma: no cover - depends on runtime env
            logging.getLogger(__name__).warning(
                "pdfminer extraction failed for %s: %s", path, exc
            )
            return ""
    return ""


def cleanup_text(text: str) -> str:
    if not text:
        return ""
    text = _legacy_cleanup_text(text)
    return preprocess_text(text, stats=None)


def separate_around_emails(text: str) -> str:
    """Historical shim: preprocessing теперь делает нужные вставки пробелов."""

    return text


def extract_text_from_pdf_bytes(
    data: bytes,
    stats: Dict[str, int] | None = None,
    budget: TimeBudget | None = None,
) -> str:
    """Read PDF bytes directly, using PyMuPDF with pdfminer fallback."""

    pages_with_text = 0
    text = ""

    if FITZ_OK and fitz is not None:
        doc = None
        try:
            doc = fitz.open(stream=data, filetype="pdf")
        except Exception:
            doc = None
        if doc is not None:
            try:
                text, pages_with_text = _collect_fitz_text(doc, budget)
            finally:
                try:
                    doc.close()
                except Exception:
                    pass

    if not text.strip() and _PDFMINER_AVAILABLE:
        try:
            from pdfminer.high_level import extract_text as pdfminer_extract
        except Exception:
            pdfminer_extract = None
        if pdfminer_extract is not None:
            heartbeat_now()
            if budget:
                budget.checkpoint()
            try:
                text_pdfminer = pdfminer_extract(io.BytesIO(data)) or ""
            except Exception:
                text_pdfminer = ""
            if text_pdfminer.strip():
                text = text_pdfminer
                pages_with_text = max(pages_with_text, 1)

    if stats is not None and pages_with_text:
        stats["pages"] = stats.get("pages", 0) + pages_with_text

    if not text:
        return ""
    if len(text) > _PDF_TEXT_TRUNCATE_LIMIT:
        text = text[:_PDF_TEXT_TRUNCATE_LIMIT]
    return cleanup_text(text)


def extract_text_from_pdf(path: str | Path) -> str:
    pdf_path = Path(path)

    text = ""
    pages = 0
    for backend in _backend_order():
        if backend == "fitz":
            text, pages = _fitz_extract_with_stats(pdf_path)
        elif backend == "pdfminer":
            text, pages = _pdfminer_extract_with_stats(pdf_path)
        else:
            text, pages = "", 0
        if text and text.strip():
            break
    if not text or not text.strip():
        fallback = _extract_with_pypdf(pdf_path)
        text = fallback if fallback.strip() else ""
    if not text:
        return ""
    if len(text) > _PDF_TEXT_TRUNCATE_LIMIT:
        text = text[:_PDF_TEXT_TRUNCATE_LIMIT]
    return cleanup_text(text)


def extract_text(
    path: str,
    stats: Dict[str, int] | None = None,
    budget: TimeBudget | None = None,
) -> str:
    """Упрощённое извлечение текста для ``emailbot.extraction``."""

    pdf_path = Path(path)

    text_fitz, pages_fitz = _fitz_extract_with_stats(pdf_path, budget)
    if text_fitz and text_fitz.strip():
        if stats is not None and pages_fitz:
            stats["pages"] = stats.get("pages", 0) + pages_fitz
        return text_fitz

    text_pdfminer, pages_pdfminer = _pdfminer_extract_with_stats(pdf_path, budget)
    if text_pdfminer and text_pdfminer.strip():
        if stats is not None:
            stats["pages"] = stats.get("pages", 0) + max(pages_fitz, pages_pdfminer)
        return text_pdfminer

    if stats is not None and max(pages_fitz, pages_pdfminer):
        stats["pages"] = stats.get("pages", 0) + max(pages_fitz, pages_pdfminer)
    return ""


def extract_from_pdf(path: str, stop_event: Optional[object] = None) -> tuple[list["EmailHit"], Dict]:
    """Extract e-mail addresses from a PDF file (PyMuPDF → pdfminer fallback)."""

    from .dedupe import merge_footnote_prefix_variants, repair_footnote_singletons
    from .extraction import EmailHit, extract_emails_document, _dedupe

    settings.load()
    strict = get("STRICT_OBFUSCATION", settings.STRICT_OBFUSCATION)
    radius = get("FOOTNOTE_RADIUS_PAGES", settings.FOOTNOTE_RADIUS_PAGES)
    layout = get("PDF_LAYOUT_AWARE", settings.PDF_LAYOUT_AWARE)
    ocr_available, ocr_configured, _ = _detect_ocr_status()
    ocr = ocr_configured and ocr_available
    join_hyphen_breaks = get("PDF_JOIN_HYPHEN_BREAKS", True)
    join_email_breaks = get("PDF_JOIN_EMAIL_BREAKS", True)

    stats: Dict[str, int] = {"pages": 0}

    _fitz = fitz if FITZ_OK else None

    def _finalize_hits(emails: List[str], source_ref: str) -> List[EmailHit]:
        raw_hits = [
            EmailHit(email=e, source_ref=source_ref, origin="direct_at")
            for e in emails
        ]
        if not raw_hits:
            return []
        merged = merge_footnote_prefix_variants(raw_hits, stats)
        merged, fstats = repair_footnote_singletons(merged, layout)
        for key, value in fstats.items():
            if value:
                stats[key] = stats.get(key, 0) + value
        return _dedupe(merged)

    def _prepare_text(raw: str) -> str:
        if not raw:
            return ""
        prepared = _maybe_join_pdf_breaks(
            raw,
            join_hyphen=join_hyphen_breaks,
            join_email=join_email_breaks,
        )
        if len(prepared) > _PDF_TEXT_TRUNCATE_LIMIT:
            prepared = prepared[:_PDF_TEXT_TRUNCATE_LIMIT]
            stats["pdf_text_truncated"] = stats.get("pdf_text_truncated", 0) + 1
        return prepared

    pdf_path = Path(path)
    text = ""
    pages_with_text = 0
    for backend in _backend_order():
        if backend == "fitz":
            text, pages_with_text = _fitz_extract_with_stats(pdf_path)
        elif backend == "pdfminer":
            text, pages_with_text = _pdfminer_extract_with_stats(pdf_path)
        else:
            text, pages_with_text = "", 0
        if text and text.strip():
            break

    if text and text.strip():
        if pages_with_text:
            stats["pages"] = stats.get("pages", 0) + pages_with_text
        prepared = _prepare_text(text)
        hits = _finalize_hits(
            extract_emails_document(prepared, stats),
            f"pdf:{path}",
        )
        return hits, stats

    if _fitz is None:
        try:
            with open(path, "rb") as f:
                text = f.read().decode("utf-8", "ignore")
        except Exception:
            return [], {"errors": ["cannot open"]}
        prepared = _prepare_text(text)
        hits = _finalize_hits(
            extract_emails_document(prepared, stats),
            f"pdf:{path}",
        )
        return hits, stats

    hits: List[EmailHit] = []
    doc = _fitz.open(path)
    ocr_pages = 0
    ocr_start = time.time()
    ocr_marked = False
    for page_idx, page in enumerate(doc, start=1):
        heartbeat_now()
        if should_stop() or (
            stop_event and getattr(stop_event, "is_set", lambda: False)()
        ):
            break
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
                if not ocr_marked:
                    stats["needs_ocr"] = stats.get("needs_ocr", 0) + 1
                    ocr_marked = True
                text = _ocr_page(page)
                if text:
                    ocr_pages += 1
                    stats["ocr_pages"] = ocr_pages
        if not text or not text.strip():
            continue
        stats["pages"] = stats.get("pages", 0) + 1
        text = _maybe_join_pdf_breaks(
            text,
            join_hyphen=join_hyphen_breaks,
            join_email=join_email_breaks,
        )
        if len(text) > _PDF_TEXT_TRUNCATE_LIMIT:
            text = text[:_PDF_TEXT_TRUNCATE_LIMIT]
            stats["pdf_text_truncated"] = stats.get("pdf_text_truncated", 0) + 1

        quick_matches = _quick_email_matches(text)
        fast_mode = len(quick_matches) >= _PDF_FAST_MIN_HITS
        if fast_mode:
            fast_norms: set[str] = set()
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
            continue

        fast_norms = {
            norm
            for raw_email, _, _ in quick_matches
            if (norm := normalize_email(raw_email))
        }
        text = _legacy_cleanup_text(text)
        text = preprocess_text(text, stats)
        low_text = text.lower()
        for email in extract_emails_document(text, stats):
            norm = normalize_email(email)
            if norm and fast_mode and norm in fast_norms:
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
    ocr_available, ocr_configured, _ = _detect_ocr_status()
    ocr = ocr_configured and ocr_available
    join_hyphen_breaks = get("PDF_JOIN_HYPHEN_BREAKS", True)
    join_email_breaks = get("PDF_JOIN_EMAIL_BREAKS", True)

    stats: Dict[str, int] = {"pages": 0}

    fitz_local = fitz if FITZ_OK else None

    def _finalize_hits(emails: List[str], ref: str) -> List[EmailHit]:
        raw_hits = [
            EmailHit(email=e, source_ref=ref, origin="direct_at") for e in emails
        ]
        if not raw_hits:
            return []
        merged = merge_footnote_prefix_variants(raw_hits, stats)
        merged, fstats = repair_footnote_singletons(merged, layout)
        for key, value in fstats.items():
            if value:
                stats[key] = stats.get(key, 0) + value
        return _dedupe(merged)

    def _prepare_text(raw: str) -> str:
        if not raw:
            return ""
        prepared = _maybe_join_pdf_breaks(
            raw,
            join_hyphen=join_hyphen_breaks,
            join_email=join_email_breaks,
        )
        if len(prepared) > _PDF_TEXT_TRUNCATE_LIMIT:
            prepared = prepared[:_PDF_TEXT_TRUNCATE_LIMIT]
            stats["pdf_text_truncated"] = stats.get("pdf_text_truncated", 0) + 1
        return prepared

    text = ""
    pages_with_text = 0

    if fitz_local is not None:
        doc_for_text = None
        try:
            doc_for_text = fitz_local.open(stream=data, filetype="pdf")
        except Exception:
            doc_for_text = None
        if doc_for_text is not None:
            try:
                text, pages_with_text = _collect_fitz_text(doc_for_text)
            finally:
                try:
                    doc_for_text.close()
                except Exception:
                    pass

    if not text.strip():
        text_pdfminer, pages_pdfminer = _pdfminer_extract_bytes_with_stats(data)
        if text_pdfminer.strip():
            text = text_pdfminer
            pages_with_text = max(pages_with_text, pages_pdfminer)

    if text and text.strip():
        if pages_with_text:
            stats["pages"] = stats.get("pages", 0) + pages_with_text
        prepared = _prepare_text(text)
        hits = _finalize_hits(
            extract_emails_document(prepared, stats),
            source_ref,
        )
        return hits, stats

    if fitz_local is None:
        try:
            text = data.decode("utf-8", "ignore")
        except Exception:
            return [], {"errors": ["cannot open"]}
        prepared = _prepare_text(text)
        hits = _finalize_hits(
            extract_emails_document(prepared, stats),
            source_ref,
        )
        return hits, stats

    hits: List[EmailHit] = []
    doc = fitz_local.open(stream=data, filetype="pdf")
    ocr_pages = 0
    ocr_start = time.time()
    ocr_marked = False
    for page_idx, page in enumerate(doc, start=1):
        if should_stop() or (
            stop_event and getattr(stop_event, "is_set", lambda: False)()
        ):
            break
        stats["pages"] = stats.get("pages", 0) + 1
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
                if not ocr_marked:
                    stats["needs_ocr"] = stats.get("needs_ocr", 0) + 1
                    ocr_marked = True
                text = _ocr_page(page)
                if text:
                    ocr_pages += 1
                    stats["ocr_pages"] = ocr_pages
        if not text or not text.strip():
            continue
        text = _maybe_join_pdf_breaks(
            text,
            join_hyphen=join_hyphen_breaks,
            join_email=join_email_breaks,
        )
        if len(text) > _PDF_TEXT_TRUNCATE_LIMIT:
            text = text[:_PDF_TEXT_TRUNCATE_LIMIT]
            stats["pdf_text_truncated"] = stats.get("pdf_text_truncated", 0) + 1

        quick_matches = _quick_email_matches(text)
        fast_mode = len(quick_matches) >= _PDF_FAST_MIN_HITS
        if fast_mode:
            fast_norms: set[str] = set()
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
            continue

        fast_norms = {
            norm
            for raw_email, _, _ in quick_matches
            if (norm := normalize_email(raw_email))
        }
        text = _legacy_cleanup_text(text)
        text = preprocess_text(text, stats)
        low_text = text.lower()
        for email in extract_emails_document(text, stats):
            norm = normalize_email(email)
            if norm and fast_mode and norm in fast_norms:
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
    "extract_text_from_pdf_bytes",
    "extract_text_from_pdf",
    "extract_text",
    "extract_from_pdf",
    "extract_from_pdf_stream",
]
