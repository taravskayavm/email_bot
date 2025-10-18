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


def test_filter_invalid_tld_ocr_repairs_spaces_and_tld():
    emails = [
        "name@domain . ru",
        "user@example.c0m",
        "broken@invalid.zz",
    ]
    stats_hint = {"ocr_pages": 2}
    valid, extra = filter_invalid_tld(emails, stats=stats_hint)
    assert len(valid) == 2
    assert sorted(valid) == ["name@domain.ru", "user@example.com"]
    # The invalid domain should still be reported and sample limited to 3
    assert extra["invalid_tld"] == 1
    assert extra["invalid_tld_examples"] == ["broken@invalid.zz"]
    assert extra["replacements"] == {
        "name@domain . ru": "name@domain.ru",
        "user@example.c0m": "user@example.com",
    }


def test_smart_extract_skips_unknown_tld():
    text = "+m@h.abs a.d@a.message tri@hlon.org"
    stats = {}
    assert smart_extract_emails(text, stats) == ["tri@hlon.org"]
    assert stats.get("foreign_domains") == 1


def test_classify_tld_generic_domestic_foreign():
    assert mu.classify_tld("user@gmail.com") == "generic"
    assert mu.classify_tld("office@rustriathlon.ru") == "domestic"
    assert mu.classify_tld("marcela.ss950@gmail.com.br") == "foreign"
