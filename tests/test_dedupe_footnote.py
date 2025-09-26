from emailbot.dedupe import merge_footnote_prefix_variants


class Hit:
    def __init__(self, email, source_ref="doc#p1", pre=""):
        self.email = email
        self.source_ref = source_ref
        self.pre = pre


def test_letter_prefix_is_not_trimmed():
    """Ensure letter prefixes are not misdetected as footnotes."""

    hits = [Hit("aivanov@site.ru", pre=" "), Hit("ivanov@site.ru", pre=" ")]
    out = merge_footnote_prefix_variants(hits, stats={})
    emails = sorted(h.email for h in out)
    assert emails == ["aivanov@site.ru", "ivanov@site.ru"]


def test_digit_prefix_may_be_trimmed_as_footnote():
    """Numeric prefixes should still be treated as footnotes."""

    hits = [Hit("1ivanov@site.ru", pre="¹"), Hit("ivanov@site.ru", pre=" ")]
    out = merge_footnote_prefix_variants(hits, stats={})
    emails = [h.email for h in out]
    assert emails == ["1ivanov@site.ru"]
