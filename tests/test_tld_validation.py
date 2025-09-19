from emailbot.extraction_common import filter_invalid_tld
from emailbot import messaging_utils as mu
from emailbot.extraction import smart_extract_emails


def test_filter_invalid_tld():
    emails = [
        "+m@h.abs",
        "a.d@a.message",
        "tri@hlon.org",
        "office@rustriathlon.ru",
        "marcela.ss950@gmail.com.br",
    ]
    valid, stats = filter_invalid_tld(emails)
    assert stats["invalid_tld"] == 2
    assert sorted(valid) == [
        "marcela.ss950@gmail.com.br",
        "office@rustriathlon.ru",
        "tri@hlon.org",
    ]


def test_smart_extract_skips_unknown_tld():
    text = "+m@h.abs a.d@a.message tri@hlon.org"
    stats = {}
    assert smart_extract_emails(text, stats) == []
    assert stats.get("foreign_domains") == 1


def test_classify_tld_generic_domestic_foreign():
    assert mu.classify_tld("user@gmail.com") == "generic"
    assert mu.classify_tld("office@rustriathlon.ru") == "domestic"
    assert mu.classify_tld("marcela.ss950@gmail.com.br") == "foreign"
