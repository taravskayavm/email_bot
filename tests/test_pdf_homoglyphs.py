from utils.email_clean import parse_emails_unified, extract_emails


def test_cyrillic_c_in_localpart_kept_as_c():
    # первая буква кириллицей: 'сhukanov.ev@gmail.com' → должно стать 'chukanov.ev@gmail.com'
    src = "контакт: сhukanov.ev@gmail.com"
    got = parse_emails_unified(src)
    assert got == ["chukanov.ev@gmail.com"]


def test_middle_dot_between_initials():
    # разделитель-«точка» из PDF: '·' → '.'
    src = "почта: chukanov·ev@gmail.com"
    got = parse_emails_unified(src)
    assert got == ["chukanov.ev@gmail.com"]


def test_both_issues_together():
    src = "e-mail: сhukanov·ev@gmail.com"
    # промежуточный extract_emails тоже должен поймать корректно
    assert extract_emails(src) == ["chukanov.ev@gmail.com"]
    assert parse_emails_unified(src) == ["chukanov.ev@gmail.com"]
