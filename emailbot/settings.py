"""Runtime configurable settings for the bot."""

from __future__ import annotations

from pathlib import Path
import json
import os

from . import settings_store as _store

# Default values
STRICT_OBFUSCATION: bool = True
FOOTNOTE_RADIUS_PAGES: int = 1
PDF_LAYOUT_AWARE: bool = False
ENABLE_OCR: bool = False
MAX_ASSETS: int = 8
MAX_SITEMAP_URLS: int = 200
MAX_DOCS: int = 30
PER_REQUEST_TIMEOUT: int = 15
DAILY_SEND_LIMIT: int = 300
EXTERNAL_SOURCES: dict[str, dict[str, dict[str, str]]] = {}
# UI helpers
SKIPPED_PREVIEW_LIMIT: int = int(os.getenv("SKIPPED_PREVIEW_LIMIT", "10"))
LAST_SUMMARY_DIR: str = os.getenv("LAST_SUMMARY_DIR", "var/last_summaries")
# Отчётная временная зона (используется в логах/отчётах)
REPORT_TZ: str = (os.getenv("REPORT_TZ") or "Europe/Moscow").strip() or "Europe/Moscow"


# ---- Reconcile (IMAP vs CSV) ----
RECONCILE_SINCE_DAYS: int = int(os.getenv("RECONCILE_SINCE_DAYS", "7"))

# Краулер: бюджеты и кэш
CRAWL_MAX_PAGES_PER_DOMAIN = int(os.getenv("CRAWL_MAX_PAGES_PER_DOMAIN", "50"))
CRAWL_TIME_BUDGET_SECONDS = int(os.getenv("CRAWL_TIME_BUDGET_SECONDS", "120"))
ROBOTS_CACHE_PATH = os.getenv("ROBOTS_CACHE_PATH", "var/robots_cache.json")
ROBOTS_CACHE_TTL_SECONDS = int(os.getenv("ROBOTS_CACHE_TTL_SECONDS", "86400"))

TPL_DIR = Path("templates")
LABELS_FILE = TPL_DIR / "_labels.json"


def load() -> None:
    """Load configuration from the persistent store."""

    global STRICT_OBFUSCATION, FOOTNOTE_RADIUS_PAGES, PDF_LAYOUT_AWARE, ENABLE_OCR
    global MAX_ASSETS, MAX_SITEMAP_URLS, MAX_DOCS, PER_REQUEST_TIMEOUT
    global EXTERNAL_SOURCES, DAILY_SEND_LIMIT, SKIPPED_PREVIEW_LIMIT, LAST_SUMMARY_DIR
    STRICT_OBFUSCATION = bool(_store.get("STRICT_OBFUSCATION", STRICT_OBFUSCATION))
    FOOTNOTE_RADIUS_PAGES = int(_store.get("FOOTNOTE_RADIUS_PAGES", FOOTNOTE_RADIUS_PAGES))
    PDF_LAYOUT_AWARE = bool(_store.get("PDF_LAYOUT_AWARE", PDF_LAYOUT_AWARE))
    ENABLE_OCR = bool(_store.get("ENABLE_OCR", ENABLE_OCR))
    MAX_ASSETS = int(_store.get("MAX_ASSETS", MAX_ASSETS))
    MAX_SITEMAP_URLS = int(_store.get("MAX_SITEMAP_URLS", MAX_SITEMAP_URLS))
    MAX_DOCS = int(_store.get("MAX_DOCS", MAX_DOCS))
    PER_REQUEST_TIMEOUT = int(_store.get("PER_REQUEST_TIMEOUT", PER_REQUEST_TIMEOUT))
    DAILY_SEND_LIMIT = int(_store.get("DAILY_SEND_LIMIT", DAILY_SEND_LIMIT))
    EXTERNAL_SOURCES = _store.get("EXTERNAL_SOURCES", EXTERNAL_SOURCES) or {}
    SKIPPED_PREVIEW_LIMIT = int(
        _store.get("SKIPPED_PREVIEW_LIMIT", SKIPPED_PREVIEW_LIMIT)
    )
    LAST_SUMMARY_DIR = str(_store.get("LAST_SUMMARY_DIR", LAST_SUMMARY_DIR))


def save() -> None:
    """Persist current configuration."""

    _store.set("STRICT_OBFUSCATION", STRICT_OBFUSCATION)
    _store.set("FOOTNOTE_RADIUS_PAGES", FOOTNOTE_RADIUS_PAGES)
    _store.set("PDF_LAYOUT_AWARE", PDF_LAYOUT_AWARE)
    _store.set("ENABLE_OCR", ENABLE_OCR)
    _store.set("MAX_ASSETS", MAX_ASSETS)
    _store.set("MAX_SITEMAP_URLS", MAX_SITEMAP_URLS)
    _store.set("MAX_DOCS", MAX_DOCS)
    _store.set("PER_REQUEST_TIMEOUT", PER_REQUEST_TIMEOUT)
    _store.set("DAILY_SEND_LIMIT", DAILY_SEND_LIMIT)
    _store.set("EXTERNAL_SOURCES", EXTERNAL_SOURCES)
    _store.set("SKIPPED_PREVIEW_LIMIT", SKIPPED_PREVIEW_LIMIT)
    _store.set("LAST_SUMMARY_DIR", LAST_SUMMARY_DIR)


def _load_labels() -> dict[str, dict[str, str]]:
    if not LABELS_FILE.exists():
        return {}
    try:
        data = json.loads(LABELS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(data, dict):
        return data
    return {}


def list_available_directions() -> list[str]:
    """Return direction labels from templates/_labels.json."""

    labels = _load_labels()
    return [str(meta.get("label") or slug) for slug, meta in labels.items()]


def resolve_label(label: str) -> str:
    """Resolve human-readable label back to slug."""

    query = label.strip()
    for slug, meta in _load_labels().items():
        stored_label = str(meta.get("label") or "").strip()
        if stored_label == query:
            return slug
    return query


# Load settings on module import.
load()


__all__ = [
    "STRICT_OBFUSCATION",
    "FOOTNOTE_RADIUS_PAGES",
    "PDF_LAYOUT_AWARE",
    "ENABLE_OCR",
    "MAX_ASSETS",
    "MAX_SITEMAP_URLS",
    "MAX_DOCS",
    "PER_REQUEST_TIMEOUT",
    "DAILY_SEND_LIMIT",
    "EXTERNAL_SOURCES",
    "SKIPPED_PREVIEW_LIMIT",
    "LAST_SUMMARY_DIR",
    "REPORT_TZ",
    "RECONCILE_SINCE_DAYS",
    "CRAWL_MAX_PAGES_PER_DOMAIN",
    "CRAWL_TIME_BUDGET_SECONDS",
    "ROBOTS_CACHE_PATH",
    "ROBOTS_CACHE_TTL_SECONDS",
    "load",
    "save",
    "list_available_directions",
    "resolve_label",
]

