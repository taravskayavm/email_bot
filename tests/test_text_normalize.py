from emailbot.text_normalize import normalize_text_for_emails


def test_normalize_glues_hyphenated_words():
    raw = "скорост-\nных"  # перенос по дефису внутри слова
    assert normalize_text_for_emails(raw) == "скоростных"


def test_normalize_replaces_newlines_with_spaces():
    raw = "первая\nвторая"
    assert normalize_text_for_emails(raw) == "первая вторая"


def test_normalize_inserts_space_before_email_token():
    raw = "Россияnik@example.com"
    assert normalize_text_for_emails(raw) == "Россия nik@example.com"
