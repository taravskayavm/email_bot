import unicodedata

from utils.email_clean import sanitize_email as _sanitize_email


def test_unicode_domain_punycode_local_nfc():
    # регресс: домен должен кодироваться как IDNA без подмен букв
    got, reason = _sanitize_email("test@тест.рф")
    assert got == ""
    assert reason == "tld-not-allowed"


def test_cyrillic_domain_to_punycode():
    got, reason = _sanitize_email("login@почта.рф")
    assert got == ""
    assert reason == "tld-not-allowed"


def test_mixed_domain_idna():
    got, reason = _sanitize_email("user@пример.com")
    assert reason is None
    assert got == "user@xn--e1afmkfd.com"


def test_idna_failure_returns_reason():
    got, reason = _sanitize_email("user@пример-.com")
    assert got == ""
    assert reason == "invalid-idna"
