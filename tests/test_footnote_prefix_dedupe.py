import pytest

from pipelines.extract_emails import extract_emails_pipeline


def test_footnote_prefixed_duplicate_removed(monkeypatch):
    monkeypatch.setenv("SUSPECTS_REQUIRE_CONFIRM", "1")
    text = "Контакты: 1ivanov@mail.ru; ivanov@mail.ru"
    allowed, meta = extract_emails_pipeline(text)
    assert "ivanov@mail.ru" in allowed
    assert all(not addr.startswith("1ivanov@") for addr in allowed)
    assert meta["suspicious_count"] == 0
