from utils.email_text_fix import fix_email_text


def test_fix_email_text_joins_breaks_and_invisibles() -> None:
    raw = "ivanov@\u200Bhema\u00ADtology . ru\nsidorov @ example . com"
    fixed = fix_email_text(raw)
    assert "ivanov@hematology.ru" in fixed
    assert "sidorov@example.com" in fixed
