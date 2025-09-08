import unicodedata

from utils.email_clean import sanitize_email


def test_unicode_domain_punycode_local_nfc():
    got = sanitize_email("test@почта.рф")
    assert got == "test@xn--80a1acny.xn--p1ai"
    assert unicodedata.is_normalized("NFC", got.split("@", 1)[0])
