"""PDF extraction helpers with optional layout and OCR features."""
from __future__ import annotations

import logging
import re
import statistics
import time
from typing import Dict, List, Optional

from emailbot import settings
from emailbot.settings_store import get
from .extraction_common import preprocess_text

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

logger = logging.getLogger(__name__)

_SOFT_HYPH = "\u00AD"


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
        return ""
    try:
        pix = page.get_pixmap(dpi=200)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        return pytesseract.image_to_string(img)
    except Exception:
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
        hits = [
            EmailHit(email=e, source_ref=f"pdf:{path}", origin="direct_at")
            for e in extract_emails_document(text, stats)
        ]
        return _dedupe(hits), {"pages": 0, "needs_ocr": True}

    hits: List[EmailHit] = []
    stats: Dict[str, int] = {"pages": 0}
    doc = fitz.open(path)
    ocr_pages = 0
    ocr_start = time.time()
    for page_idx, page in enumerate(doc, start=1):
        if stop_event and getattr(stop_event, "is_set", lambda: False)():
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
        text = preprocess_text(text, stats)
        low_text = text.lower()
        for email in extract_emails_document(text, stats):
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
        if stop_event and getattr(stop_event, "is_set", lambda: False)():
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
        hits = [
            EmailHit(email=e, source_ref=source_ref, origin="direct_at")
            for e in extract_emails_document(text, stats)
        ]
        return _dedupe(hits), {"pages": 0, "needs_ocr": True}

    hits: List[EmailHit] = []
    stats: Dict[str, int] = {"pages": 0}
    doc = fitz.open(stream=data, filetype="pdf")
    ocr_pages = 0
    ocr_start = time.time()
    for page_idx, page in enumerate(doc, start=1):
        if stop_event and getattr(stop_event, "is_set", lambda: False)():
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
        text = preprocess_text(text, stats)
        low_text = text.lower()
        for email in extract_emails_document(text, stats):
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
        if stop_event and getattr(stop_event, "is_set", lambda: False)():
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


__all__ = ["extract_from_pdf", "extract_from_pdf_stream"]
