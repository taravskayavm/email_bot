"""Runtime configurable settings for the bot."""

from __future__ import annotations

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
EXTERNAL_SOURCES: dict[str, dict[str, dict[str, str]]] = {}


def load() -> None:
    """Load configuration from the persistent store."""

    global STRICT_OBFUSCATION, FOOTNOTE_RADIUS_PAGES, PDF_LAYOUT_AWARE, ENABLE_OCR
    global MAX_ASSETS, MAX_SITEMAP_URLS, MAX_DOCS, PER_REQUEST_TIMEOUT, EXTERNAL_SOURCES
    STRICT_OBFUSCATION = bool(_store.get("STRICT_OBFUSCATION", STRICT_OBFUSCATION))
    FOOTNOTE_RADIUS_PAGES = int(_store.get("FOOTNOTE_RADIUS_PAGES", FOOTNOTE_RADIUS_PAGES))
    PDF_LAYOUT_AWARE = bool(_store.get("PDF_LAYOUT_AWARE", PDF_LAYOUT_AWARE))
    ENABLE_OCR = bool(_store.get("ENABLE_OCR", ENABLE_OCR))
    MAX_ASSETS = int(_store.get("MAX_ASSETS", MAX_ASSETS))
    MAX_SITEMAP_URLS = int(_store.get("MAX_SITEMAP_URLS", MAX_SITEMAP_URLS))
    MAX_DOCS = int(_store.get("MAX_DOCS", MAX_DOCS))
    PER_REQUEST_TIMEOUT = int(_store.get("PER_REQUEST_TIMEOUT", PER_REQUEST_TIMEOUT))
    EXTERNAL_SOURCES = _store.get("EXTERNAL_SOURCES", EXTERNAL_SOURCES) or {}


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
    _store.set("EXTERNAL_SOURCES", EXTERNAL_SOURCES)


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
    "EXTERNAL_SOURCES",
    "load",
    "save",
]

