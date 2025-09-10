from utils.email_clean import parse_emails_unified, extract_emails


def test_cyrillic_soft_sign_to_b_in_localpart():
    # должно стать 'balan7@yandex.ru'
    src = "контакт: ьalan7@yandex.ru"
    got = parse_emails_unified(src)
    assert got == ["balan7@yandex.ru"]


def test_cyrillic_c_and_middle_dot_to_dot():
    src = "e-mail: сhukanov·ev@gmail.com"
    assert parse_emails_unified(src) == ["chukanov.ev@gmail.com"]


def test_extract_emails_after_normalize_localparts():
    src = "почта: сhukanov·ev@gmail.com"
    assert extract_emails(src) == ["chukanov.ev@gmail.com"]
