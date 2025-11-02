"""Regression tests for timeouts and archive safeguards."""

from __future__ import annotations

import zipfile

from emailbot.extraction_pdf import extract_from_pdf
from emailbot.extraction_zip import extract_from_zip
from emailbot.utils.file_email_extractor import extract_from_plain_text


def test_email_regex_chunking_no_hang() -> None:
    big = ("x" * (256 * 1024)) + " test@example.com "
    emails = extract_from_plain_text(big)
    assert "test@example.com" in emails


def test_pdf_timeout_no_crash(tmp_path) -> None:
    pdf_path = tmp_path / "fake.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%EOF\n" + b"\x00" * (2 * 1024 * 1024))
    out = extract_from_pdf(str(pdf_path), timeout_sec=1, max_pages=3)
    assert isinstance(out, str)


def test_zip_limits_no_crash(tmp_path) -> None:
    zip_path = tmp_path / "many.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for idx in range(600):
            archive.writestr(f"f{idx}.txt", "x" * 16)
    members = extract_from_zip(str(zip_path), timeout_sec=1)
    assert isinstance(members, list)
