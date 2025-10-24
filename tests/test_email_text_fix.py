import importlib

import utils.email_text_fix as email_text_fix


def test_fix_email_text_joins_breaks_and_invisibles() -> None:
    raw = "ivanov@\u200Bhema\u00ADtology . ru\nsidorov @ example . com"
    fixed = email_text_fix.fix_email_text(raw)
    assert "ivanov@hematology.ru" in fixed
    assert "sidorov@example.com" in fixed


def test_fix_email_text_preserves_regular_dog_words() -> None:
    raw = "Bulldog report\nhot dog stand\ninfo (dog) example dot com"
    fixed = email_text_fix.fix_email_text(raw)
    assert "Bulldog" in fixed
    assert "hot dog" in fixed
    assert "info@example" in fixed


def test_fix_email_text_respects_join_flags(monkeypatch) -> None:
    text = "user-\nname@example.\ncom"
    fixed = email_text_fix.fix_email_text(
        text, join_email_breaks=False, join_hyphen_breaks=False
    )
    assert "user-\nname@example.\ncom" in fixed


def test_fix_email_text_defaults_follow_pdf_join_env(monkeypatch) -> None:
    monkeypatch.setenv("PDF_JOIN_EMAIL_BREAKS", "0")
    monkeypatch.setenv("PDF_JOIN_HYPHEN_BREAKS", "0")
    importlib.reload(email_text_fix)

    text = "ivanov-\npetrov@example.\ncom"
    fixed = email_text_fix.fix_email_text(text)
    assert "ivanov-\npetrov@example.\ncom" in fixed
