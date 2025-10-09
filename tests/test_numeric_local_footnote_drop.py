from emailbot.extraction import EmailHit, _postprocess_hits


def _hit(email: str, origin: str = "document", source_ref: str = "doc#p1", pre: str = "", post: str = "", meta=None) -> EmailHit:
    return EmailHit(
        email=email,
        source_ref=source_ref,
        origin=origin,
        pre=pre,
        post=post,
        meta=meta or {},
    )


def test_short_numeric_localparts_are_dropped() -> None:
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


def test_three_digit_numeric_local_is_kept() -> None:
    hits = [_hit("123@dept.example.com"), _hit("12@dept.example.com")]
    stats: dict[str, int] = {}
    out = _postprocess_hits(hits, stats)
    emails = sorted(h.email for h in out)
    assert "123@dept.example.com" in emails
    assert "12@dept.example.com" not in emails
