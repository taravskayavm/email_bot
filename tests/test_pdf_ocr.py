import threading
from pathlib import Path
import types

import pytest

import emailbot.extraction_pdf as ep


def _create_blank_pdf(path: Path) -> None:
    import fitz  # type: ignore

    doc = fitz.open()
    doc.new_page()
    doc.save(path)


def test_ocr_toggle(monkeypatch, tmp_path: Path):
    pdf = tmp_path / "scan.pdf"
    _create_blank_pdf(pdf)

    monkeypatch.setattr(ep, "_ocr_page", lambda page: "scan@example.com")
    monkeypatch.setattr(ep, "preprocess_text", lambda t: t)

    settings = types.SimpleNamespace(
        PDF_LAYOUT_AWARE=False,
        ENABLE_OCR=False,
        STRICT_OBFUSCATION=True,
        FOOTNOTE_RADIUS_PAGES=1,
        load=lambda: None,
    )
    monkeypatch.setattr(ep, "settings", settings)
    monkeypatch.setattr(ep, "get", lambda key, default=None: getattr(settings, key, default))

    hits, stats = ep.extract_from_pdf(str(pdf))
    assert [h.email for h in hits] == []

    settings.ENABLE_OCR = True
    hits, stats = ep.extract_from_pdf(str(pdf))
    assert [h.email for h in hits] == ["scan@example.com"]
    assert stats["ocr_pages"] == 1


def test_stop_event(monkeypatch, tmp_path: Path):
    pdf = tmp_path / "scan.pdf"
    _create_blank_pdf(pdf)

    monkeypatch.setattr(ep, "_ocr_page", lambda page: "")
    settings = types.SimpleNamespace(
        PDF_LAYOUT_AWARE=False,
        ENABLE_OCR=True,
        STRICT_OBFUSCATION=True,
        FOOTNOTE_RADIUS_PAGES=1,
        load=lambda: None,
    )
    monkeypatch.setattr(ep, "settings", settings)
    monkeypatch.setattr(ep, "get", lambda key, default=None: getattr(settings, key, default))

    event = threading.Event()
    event.set()
    hits, stats = ep.extract_from_pdf(str(pdf), stop_event=event)
    assert stats["pages"] == 0
