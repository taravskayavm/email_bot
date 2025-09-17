"""Runtime configurable settings for the bot."""

from __future__ import annotations

from pathlib import Path
import json

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

TPL_DIR = Path("templates")
LABELS_FILE = TPL_DIR / "_labels.json"


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
    "EXTERNAL_SOURCES",
    "load",
    "save",
    "list_available_directions",
    "resolve_label",
]

