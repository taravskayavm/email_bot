"""Runtime configurable settings for the bot."""

from __future__ import annotations

from . import settings_store as _store

# Default values
STRICT_OBFUSCATION: bool = True
FOOTNOTE_RADIUS_PAGES: int = 1
PDF_LAYOUT_AWARE: bool = False
ENABLE_OCR: bool = False


def load() -> None:
    """Load configuration from the persistent store."""

    global STRICT_OBFUSCATION, FOOTNOTE_RADIUS_PAGES, PDF_LAYOUT_AWARE, ENABLE_OCR
    STRICT_OBFUSCATION = bool(_store.get("STRICT_OBFUSCATION", STRICT_OBFUSCATION))
    FOOTNOTE_RADIUS_PAGES = int(_store.get("FOOTNOTE_RADIUS_PAGES", FOOTNOTE_RADIUS_PAGES))
    PDF_LAYOUT_AWARE = bool(_store.get("PDF_LAYOUT_AWARE", PDF_LAYOUT_AWARE))
    ENABLE_OCR = bool(_store.get("ENABLE_OCR", ENABLE_OCR))


def save() -> None:
    """Persist current configuration."""

    _store.set("STRICT_OBFUSCATION", STRICT_OBFUSCATION)
    _store.set("FOOTNOTE_RADIUS_PAGES", FOOTNOTE_RADIUS_PAGES)
    _store.set("PDF_LAYOUT_AWARE", PDF_LAYOUT_AWARE)
    _store.set("ENABLE_OCR", ENABLE_OCR)


# Load settings on module import.
load()


__all__ = [
    "STRICT_OBFUSCATION",
    "FOOTNOTE_RADIUS_PAGES",
    "PDF_LAYOUT_AWARE",
    "ENABLE_OCR",
    "load",
    "save",
]

