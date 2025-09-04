import pytest

from emailbot.extraction import smart_extract_emails


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("\u00b9ivanov@uni.edu", ["ivanov@uni.edu"]),
        ("1petrov@uni.edu", ["petrov@uni.edu"]),
        ("apetrov@uni.edu", ["petrov@uni.edu"]),
        ("aivanov@uni.edu", ["ivanov@uni.edu"]),
        ("name-name@dept.domain.co.uk", ["name-name@dept.domain.co.uk"]),
        (
            "user+tag_2024%eq=ok/part'one~x@sub-domain.xn--80asehdb",
            ["user+tag_2024%eq=ok/part'one~x@sub-domain.xn--80asehdb"],
        ),
        ("na.me+tag@domain.ru", ["na.me+tag@domain.ru"]),
        ("name-\nname@domain.ru", ["namename@domain.ru"]),
        ("na\nme@domain.ru", ["name@domain.ru"]),
        ("mail@uni.ru\u0434\u043e\u0446\u0435\u043d\u0442", ["mail@uni.ru"]),
        ("mail@domain.rufaculty", ["mail@domain.ru"]),
    ],
)
def test_positive(raw, expected):
    assert smart_extract_emails(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "name@domain",
        "na..me@domain.ru",
        ".name@domain.ru",
        "name.@domain.ru",
        "name@-domain.ru",
        "name@domain-.ru",
        '"a b"@domain.ru',
    ],
)
def test_negative(raw):
    assert smart_extract_emails(raw) == []


def test_preprocess_preserves_digits():
    from emailbot.extraction_common import preprocess_text

    assert preprocess_text("9\n6soul@mail.ru").startswith("9\n6soul")
    assert preprocess_text("name-\nname@domain.ru").startswith("namename@domain.ru")
    assert preprocess_text("name\u00ADname@domain.ru").startswith("namename@domain.ru")
