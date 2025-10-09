from emailbot.extraction import EmailHit, _postprocess_hits


def _hit(email: str) -> EmailHit:
    return EmailHit(email=email, source_ref="doc#p1", origin="document", pre="", post="", meta={})


def test_short_numeric_localparts_are_dropped():
    hits = [
        _hit("1@gmail.com"),
        _hit("0@yandex.ru"),
        _hit("5@mail.ru"),
        _hit("author.name@uni.ru"),
        _hit("ab2cd@mail.ru"),
    ]
    stats: dict[str, int] = {}
    out = _postprocess_hits(hits, stats)
    emails = sorted(h.email for h in out)
    assert emails == ["ab2cd@mail.ru", "author.name@uni.ru"]
    assert stats.get("dropped_numeric_local_1_2") == 3


def test_three_digit_numeric_local_is_kept():
    hits = [
        _hit("123@example.org"),
        _hit("12@example.org"),
    ]
    stats: dict[str, int] = {}
    out = _postprocess_hits(hits, stats)
    emails = sorted(h.email for h in out)
    assert "123@example.org" in emails
    assert "12@example.org" not in emails
