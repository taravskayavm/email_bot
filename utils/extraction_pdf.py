"""Compatibility shim for historical imports."""

from emailbot.extraction_pdf import (
    BASIC_EMAIL,
    INVISIBLES,
    SUPERSCRIPTS,
    cleanup_text,
    extract_text_from_pdf,
    separate_around_emails,
)

__all__ = [
    "BASIC_EMAIL",
    "INVISIBLES",
    "SUPERSCRIPTS",
    "cleanup_text",
    "extract_text_from_pdf",
    "separate_around_emails",
]
