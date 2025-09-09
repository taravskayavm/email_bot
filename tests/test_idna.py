import unicodedata

from utils.email_clean import sanitize_email


def test_unicode_domain_punycode_local_nfc():
    # регресс: домен должен кодироваться как IDNA без подмен букв
    raw = "test@тест.рф"
    got = sanitize_email(raw)
    assert got == "test@xn--e1aybc.xn--p1ai"
    assert unicodedata.is_normalized("NFC", got.split("@", 1)[0])
