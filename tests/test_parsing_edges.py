from utils.email_clean import parse_emails_unified, sanitize_email

def test_trailing_glue_after_tld_is_cut():
    s = "omgma-obschim@mail.ruSysoev"
    assert parse_emails_unified(s) == ["omgma-obschim@mail.ru"]

def test_leading_and_double_dots_are_normalized():
    assert sanitize_email(".a.v@vniifk.ru") == "a.v@vniifk.ru"
    assert sanitize_email("nauka.vgifk.@mail.ru") == "nauka.vgifk@mail.ru"
    assert sanitize_email("ab..cd@mail.ru") == "ab.cd@mail.ru"

def test_invisibles_and_hyphenation():
    assert sanitize_email("\xadandrew@mail.ru") == "andrew@mail.ru"
    assert sanitize_email("an-\ndrew@mail.ru") == "andrew@mail.ru"
