from utils.email_clean import extract_emails, parse_emails_unified


def test_no_first_char_loss_after_word_boundary():
    # буква "b" не должна пропадать
    src = "ФИОbalan7@yandex.ru"
    assert extract_emails(src) == ["balan7@yandex.ru"]
    assert parse_emails_unified(src) == ["balan7@yandex.ru"]


def test_no_first_char_loss_with_soft_hyphen_before():
    # soft hyphen (невидимый перенос) перед локалом
    src = "контакт:­balan7@yandex.ru"
    assert parse_emails_unified(src) == ["balan7@yandex.ru"]


def test_no_first_char_loss_with_zwsp_before():
    # zero-width space перед локалом
    src = "контакт:​balan7@yandex.ru"
    assert parse_emails_unified(src) == ["balan7@yandex.ru"]


def test_two_char_numeric_local_preserved_after_boundary():
    src = ", 1a@example.com"
    expected = ["1a@example.com"]
    assert extract_emails(src) == expected
    assert parse_emails_unified(src) == expected
