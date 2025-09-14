from utils.email_clean import dedupe_with_variants, sanitize_email


def test_strip_leading_superscript_footnote_only():
    assert sanitize_email("\u00B9test@site.com") == "test@site.com"  # ¹test
    assert sanitize_email("\u2075\u2075alex@example.com") == "alex@example.com"  # ⁵⁵alex
    assert sanitize_email("1test@site.com") == "1test@site.com"
    assert sanitize_email("55alex@example.com") == "55alex@example.com"


def test_trim_local_part_edges():
    assert sanitize_email("..test-@site.com") == "test@site.com"
    assert sanitize_email("__user__@example.org") == "user@example.org"


def test_dedupe_with_variants_prefers_clean():
    got = dedupe_with_variants(["¹alex@example.com", "alex@example.com"])
    assert got == ["alex@example.com"]


def test_dedupe_only_variants_keeps_shortest():
    got = dedupe_with_variants(["¹²³alex@example.com", "⁹alex@example.com"])
    assert got == ["alex@example.com"]
