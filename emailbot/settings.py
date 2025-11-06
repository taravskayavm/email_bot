"""Runtime configurable settings for the bot."""

from __future__ import annotations

try:
    settings  # type: ignore[name-defined]
except NameError:
    import os
    from types import SimpleNamespace

    try:
        # Import all upper-case defaults from config if present
        from .config import *  # noqa: F401,F403
    except Exception:
        pass

    def _getenv_int(key: str, default: int) -> int:
        val = os.getenv(key)
        try:
            return int(val) if val is not None and val != "" else default
        except Exception:
            return default

    def _getenv_str(key: str, default: str) -> str:
        val = os.getenv(key)
        return val if val not in (None, "") else default

    def _abspath(p: str | None) -> str | None:
        if not p:
            return None
        try:
            return os.path.abspath(p)
        except Exception:
            return p

    # Resolve stoplist path: prefer BLOCKED_EMAILS_PATH, then BLOCKED_LIST_PATH, fallback var/blocked_emails.txt
    _stoplist = _getenv_str("BLOCKED_EMAILS_PATH", _getenv_str("BLOCKED_LIST_PATH", "var/blocked_emails.txt"))
    _sent_log = _getenv_str("SENT_LOG_PATH", "var/sent_log.csv")

    # Web crawl knobs (fallbacks if not defined in config.py)
    try:
        _crawl_depth = CRAWL_MAX_DEPTH  # type: ignore[name-defined]
    except Exception:
        _crawl_depth = _getenv_int("CRAWL_MAX_DEPTH", 2)
    try:
        _crawl_pages = CRAWL_MAX_PAGES  # type: ignore[name-defined]
    except Exception:
        _crawl_pages = _getenv_int("CRAWL_MAX_PAGES", 50)
    try:
        _crawl_budget = CRAWL_TIME_BUDGET_SECONDS  # type: ignore[name-defined]
    except Exception:
        _crawl_budget = _getenv_int("CRAWL_TIME_BUDGET_SECONDS", 90)

    # PDF knobs
    _pdf_max_pages = _getenv_int("PDF_MAX_PAGES", 0)
    _pdf_ocr_auto = _getenv_int("PDF_OCR_AUTO", 1)
    _pdf_ocr_max = _getenv_int("PDF_OCR_MAX_PAGES", 100)

    # General
    _cooldown_days = _getenv_int("SEND_COOLDOWN_DAYS", 180)
    _enable_web = _getenv_int("ENABLE_WEB", 1)

    # Provider canonicalisation toggles
    _enable_provider_canon = _getenv_int("ENABLE_PROVIDER_CANON", 1)
    _canon_gmail_dots = _getenv_int("CANON_GMAIL_DOTS", 1)
    _canon_gmail_plus = _getenv_int("CANON_GMAIL_PLUS", 1)
    _canon_other_plus = _getenv_int("CANON_OTHER_PLUS", 1)

    settings = SimpleNamespace(
        # Paths
        BLOCKED_FILE=_abspath(_stoplist),
        SENT_LOG_PATH=_abspath(_sent_log),
        # Policy
        SEND_COOLDOWN_DAYS=_cooldown_days,
        # PDF/OCR
        PDF_MAX_PAGES=_pdf_max_pages,
        PDF_OCR_AUTO=_pdf_ocr_auto,
        PDF_OCR_MAX_PAGES=_pdf_ocr_max,
        # WEB
        ENABLE_WEB=_enable_web,
        CRAWL_MAX_DEPTH=_crawl_depth,
        CRAWL_MAX_PAGES=_crawl_pages,
        CRAWL_TIME_BUDGET_SECONDS=_crawl_budget,
        # Email canonicalisation
        ENABLE_PROVIDER_CANON=_enable_provider_canon,
        CANON_GMAIL_DOTS=_canon_gmail_dots,
        CANON_GMAIL_PLUS=_canon_gmail_plus,
        CANON_OTHER_PLUS=_canon_other_plus,
    )

    _exported = {name: getattr(settings, name) for name in vars(settings) if name.isupper()}
    globals().update(_exported)

__all__ = ["settings", *sorted(name for name in globals() if name.isupper())]
