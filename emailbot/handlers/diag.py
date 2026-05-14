from __future__ import annotations

from textwrap import dedent

from emailbot import config


def _flag(value: object) -> str:
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return "вкл"
    if text in {"0", "false", "no", "off"}:
        return "выкл"
    if isinstance(value, bool):
        return "вкл" if value else "выкл"
    return str(value)


def _path(value: object) -> str:
    if value is None:
        return "—"
    text = str(value).strip()
    return text or "—"


def build_pdf_ocr_settings_report() -> str:
    """Return a multi-line report with the effective PDF/OCR configuration."""

    adaptive = _flag(config.PDF_ADAPTIVE_TIMEOUT)
    enable_ocr = _flag(config.EMAILBOT_ENABLE_OCR)
    auto_ocr = _flag(config.PDF_OCR_AUTO)
    tesseract_cmd = _path(config.TESSERACT_CMD or "<auto>")

    body = dedent(
        f"""
        🧪 Диагностика PDF/OCR

        ENGINE: {config.PDF_ENGINE} | backend={config.PDF_BACKEND} | fallback={config.PDF_FALLBACK_BACKEND}
        Таймауты: адаптивный={adaptive} (base={config.PDF_TIMEOUT_BASE}s; perMB={config.PDF_TIMEOUT_PER_MB}s; min={config.PDF_TIMEOUT_MIN}s; max={config.PDF_TIMEOUT_MAX}s) | запасной={config.PDF_EXTRACT_TIMEOUT}s
        PDF_MAX_PAGES: {config.PDF_MAX_PAGES}

        OCR режим: auto={auto_ocr} | ENABLE_OCR={enable_ocr} | engine={config.PDF_OCR_ENGINE} | lang={config.PDF_OCR_LANG}
        OCR лимиты: pages≤{config.PDF_OCR_MAX_PAGES}; page_timeout={config.PDF_OCR_TIMEOUT_PER_PAGE}s; total_limit={config.PDF_OCR_TIME_LIMIT}s; dpi={config.PDF_OCR_DPI}
        OCR эвристики: probe_pages={config.PDF_OCR_PROBE_PAGES}; min_text_ratio={config.PDF_OCR_MIN_TEXT_RATIO}; min_chars={config.PDF_OCR_MIN_CHARS}; force_if_lt={config.PDF_FORCE_OCR_IF_FOUND_LT}
        Пути: cache={_path(config.PDF_OCR_CACHE_DIR)}; tesseract={tesseract_cmd}
        """
    ).strip()

    return body

