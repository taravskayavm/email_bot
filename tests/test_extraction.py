import pytest

from emailbot.extraction import smart_extract_emails


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("\u00b9ivanov@uni.com", ["ivanov@uni.com"]),
        ("1petrov@uni.com", ["petrov@uni.com"]),
        ("apetrov@uni.com", ["apetrov@uni.com"]),
        ("aivanov@uni.com", ["aivanov@uni.com"]),
        ("name-name@dept.domain.com", ["name-name@dept.domain.com"]),
        (
            "user+tag_2024%eq=ok/part'one~x@sub-domain.com",
            ["user+tag_2024%eq=ok/part'one~x@sub-domain.com"],
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


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("+7-913-331-52-25stark_velik@mail.ru", ["stark_velik@mail.ru"]),
        ("01-37-93elena-dzhioeva@yandex.ru", ["elena-dzhioeva@yandex.ru"]),
        ("18-24-40pavelshabalin@mail.ru", ["pavelshabalin@mail.ru"]),
        ("\u00b9biathlon@yandex.ru", ["biathlon@yandex.ru"]),
        ("bi@hlonrus.com", ["bi@hlonrus.com"]),
        ("20yaik11@mail.ru", ["20yaik11@mail.ru"]),
        ("6soul@mail.ru", ["6soul@mail.ru"]),
        ("89124768555@mail.ru", ["89124768555@mail.ru"]),
    ],
)
def test_phone_prefix_and_superscript(raw, expected):
    assert smart_extract_emails(raw) == expected
