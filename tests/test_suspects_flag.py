from utils.email_clean import parse_emails_unified


def test_marks_suspect_when_punct_before_and_startswith_abc():
    emails, meta = parse_emails_unified("…Россия.belyova@mail.ru", return_meta=True)
    assert emails == ["belyova@mail.ru"]
    assert meta.get("suspects") == ["belyova@mail.ru"]
