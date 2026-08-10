from utils.email_text_fix import fix_email_text


def test_fix_email_text_joins_breaks_and_invisibles() -> None:
    raw = "ivanov@\u200Bhema\u00ADtology . ru\nsidorov @ example . com"
    fixed = fix_email_text(raw)
    assert "ivanov@hematology.ru" in fixed
    assert "sidorov@example.com" in fixed


def test_fix_email_text_does_not_replace_at_inside_english_words() -> None:
    raw = (
        "Annotation. Education is important. "
        "Highly qualified acrobats in competition."
    )

    fixed = fix_email_text(raw)

    assert "@" not in fixed
    assert "Annotation" in fixed
    assert "education" in fixed.lower()
    assert "acrobats" in fixed.lower()


def test_fix_email_text_keeps_explicit_obfuscation_support() -> None:
    fixed = fix_email_text("person (at) example [dot] com")

    assert "person@example.com" in fixed
