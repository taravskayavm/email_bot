import pytest

from emailbot.extraction_pdf import _join_email_linebreaks, _join_hyphen_breaks
from utils.email_clean import parse_emails_unified


def test_pdf_hyphen_breaks_join(monkeypatch):
    monkeypatch.setenv("PDF_JOIN_HYPHEN_BREAKS", "1")
    text = "Почта: jo-\nhn@example.com, другое: te-\nst."
    result = _join_hyphen_breaks(text)
    assert "john@example.com" in result
    assert "test" in result


def test_pdf_email_linebreaks_join(monkeypatch):
    monkeypatch.setenv("PDF_JOIN_EMAIL_BREAKS", "1")
    text = "Связь: user.\nname@\nmail.\nru"
    result = _join_email_linebreaks(text)
    emails, _ = parse_emails_unified(result, return_meta=True)
    assert "user.name@mail.ru" in set(emails)
