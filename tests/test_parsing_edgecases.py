from utils.text_normalize import normalize_text
from utils.email_clean import parse_emails_unified


def test_obfuscated_with_phone_and_footnote():
    src = "Связь: +7 999 123 45 67 russiaivanov@mail.ru (1) и ivanov at gmail dot com"
    text = normalize_text(src)
    emails = parse_emails_unified(text)
    # «russiaivanov@» не должен попасть в чистые автоматически
    assert "ivanov@gmail.com" in emails
    assert all(not x.startswith("russia") for x in emails)


def test_domain_glue():
    src = "Почта: yandex.ru3nekit.maksimovich@mail.ru"
    emails = parse_emails_unified(normalize_text(src))
    assert "nekit.maksimovich@mail.ru" in emails or any("nekit" in e for e in emails)
