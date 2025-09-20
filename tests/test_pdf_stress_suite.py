"""Stress-tests for PDF extraction edge cases."""

from __future__ import annotations

from pathlib import Path

import pytest

from emailbot.extraction_pdf import extract_from_pdf_stream
from utils.tld_utils import is_allowed_domain


PDF_DIR = Path(__file__).resolve().parent / "fixtures/pdfs"


def _has_cyrillic(text: str) -> bool:
    return any("\u0400" <= char <= "\u04FF" for char in text)


@pytest.mark.skipif(not PDF_DIR.exists(), reason="no PDF fixtures provided")
def test_pdf_extract_no_glued_boundaries() -> None:
    for pdf_path in sorted(PDF_DIR.glob("*.pdf")):
        data = pdf_path.read_bytes()
        hits, stats = extract_from_pdf_stream(data, source_ref=str(pdf_path))
        cleaned = {hit.email for hit in hits}
        assert all(not _has_cyrillic(email) for email in cleaned), f"{pdf_path} has Cyrillic in emails"
        assert all(
            is_allowed_domain(email.split("@", 1)[1])
            for email in cleaned
        ), f"{pdf_path} invalid TLD"
        suspects = set(stats.get("emails_suspects") or stats.get("suspects") or [])
        assert cleaned.isdisjoint(suspects), f"{pdf_path} suspects leaked into cleaned"


@pytest.mark.skipif(not PDF_DIR.exists(), reason="no PDF fixtures provided")
def test_pdf_handles_hyphen_and_linebreaks() -> None:
    for pdf_path in sorted(PDF_DIR.glob("*.pdf")):
        hits, _ = extract_from_pdf_stream(pdf_path.read_bytes(), source_ref=str(pdf_path))
        assert len({hit.email for hit in hits}) >= 1, f"{pdf_path} yielded zero emails"
