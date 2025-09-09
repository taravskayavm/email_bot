from utils.email_clean import extract_emails, dedupe_with_variants


def test_english_obfuscation():
    src = "Write me: ivan.petrov [at] gmail [dot] com"
    assert extract_emails(src) == ["ivan.petrov@gmail.com"]


def test_russian_obfuscation():
    src = "почта: ivan(собака)yandex(точка)ru"
    assert extract_emails(src) == ["ivan@yandex.ru"]


def test_ocr_comma_before_tld():
    src = "user@mail,ru"
    assert extract_emails(src) == ["user@mail.ru"]


def test_provider_dedupe_gmail_plus_and_dots():
    lst = ["ivan.petrov+news@gmail.com", "ivanpetrov@gmail.com"]
    assert dedupe_with_variants(lst) == ["ivan.petrov+news@gmail.com"]


def test_provider_dedupe_yandex_plus():
    lst = ["pavel+tag@yandex.ru", "pavel@yandex.ru"]
    assert dedupe_with_variants(lst) == ["pavel+tag@yandex.ru"]

