import pytest

from utils.email_clean import sanitize_email, dedupe_with_variants


def test_strip_leading_footnote_in_local_part():
    assert sanitize_email("1test@site.com") == "test@site.com"
    assert sanitize_email("55alex@example.com") == "alex@example.com"


def test_trim_local_part_edges():
    assert sanitize_email("..test-@site.com") == "test@site.com"
    assert sanitize_email("__user__@example.org") == "user@example.org"


def test_dedupe_with_variants_prefers_clean():
    got = dedupe_with_variants(["55alex@example.com", "alex@example.com"])
    assert got == ["alex@example.com"]


def test_dedupe_only_variants_keeps_shortest():
    got = dedupe_with_variants(["123alex@example.com", "9alex@example.com"])
    assert got == ["9alex@example.com"]
