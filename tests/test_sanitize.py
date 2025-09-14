from utils.email_clean import dedupe_with_variants, sanitize_email


def test_superscript_footnote_in_local_part():
    assert sanitize_email("0-ju@mail.ru") == "0-ju@mail.ru"
    assert sanitize_email("¹alex@mail.ru") == "alex@mail.ru"
    assert sanitize_email("0-\nju@mail.ru") == "0-ju@mail.ru"


def test_trim_local_part_edges():
    assert sanitize_email("..test-@site.com") == "test@site.com"
    assert sanitize_email("__user__@example.org") == "user@example.org"


def test_dedupe_with_variants_prefers_clean():
    got = dedupe_with_variants(["¹alex@example.com", "alex@example.com"])
    assert got == ["alex@example.com"]


def test_dedupe_only_variants_keeps_shortest():
    got = dedupe_with_variants(["¹²³alex@example.com", "⁹alex@example.com"])
    assert got == ["alex@example.com"]
