def test_dedupe_with_footnote_and_clean():
    from utils.email_clean import dedupe_with_variants

    got = dedupe_with_variants(["¹alex@example.com", "alex@example.com"])
    assert got == ["alex@example.com"]


def test_dedupe_only_variants_keeps_shortest():
    from utils.email_clean import dedupe_with_variants

    got = dedupe_with_variants(["¹²³alex@example.com", "⁹alex@example.com"])
    assert got == ["alex@example.com"]


def test_no_cross_domain_collapse():
    from utils.email_clean import dedupe_with_variants

    got = dedupe_with_variants(["alex@a.com", "alex@b.com"])
    assert sorted(got) == ["alex@a.com", "alex@b.com"]
