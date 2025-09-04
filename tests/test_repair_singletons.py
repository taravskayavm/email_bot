import pytest

from emailbot.dedupe import repair_footnote_singletons
from emailbot.extraction import EmailHit
from emailbot.dedupe import repair_footnote_singletons


def make_hit(email: str, pre: str) -> EmailHit:
    return EmailHit(
        email=email,
        source_ref="pdf:doc.pdf#page=1",
        origin="direct_at",
        pre=pre,
        post="",
    )


def test_superscript_singleton_repaired():
    h = make_hit("1dergal@yandex.ru", pre="¹")
    res, fixed = repair_footnote_singletons([h])
    assert res[0].email == "dergal@yandex.ru"
    assert res[0].origin == "footnote_repaired"
    assert fixed == 1


def test_normal_address_untouched():
    h = make_hit("6soul@mail.ru", pre=" ")
    res, fixed = repair_footnote_singletons([h])
    assert res == [h]
    assert fixed == 0


def test_double_digit_not_repaired():
    h = make_hit("20yaik11@mail.ru", pre="2")
    res, fixed = repair_footnote_singletons([h])
    assert res == [h]
    assert fixed == 0
