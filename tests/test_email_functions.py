import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import emailbot.extraction as extraction


def test_preclean_merges_hyphen_newlines_and_spaces():
    raw = "user-\nname @ example. c o m"
    assert extraction._preclean_text_for_emails(raw) == "username@example.com"


def test_extract_clean_emails_handles_variants_and_truncations():
    text = (
        "user-\nname @ example. c o m\n"
        "info@example.org\n"
        "1john@example.com 2john@example.com\n"
        "Vilena\n33 @mail. r u"
    )
    expected = {"username@example.com", "john@example.com", "vilena33@mail.ru"}
    assert extraction.extract_clean_emails_from_text(text) == expected


@pytest.mark.parametrize(
    "candidates,expected",
    [
        ({"33@mail.ru", "vilena33@mail.ru"}, [("33@mail.ru", "vilena33@mail.ru")]),
        ({"33@mail.ru", "anna33@mail.ru", "olga33@mail.ru"}, []),
        ({"33@mail.ru"}, []),
    ],
)
def test_detect_numeric_truncations(candidates, expected):
    assert sorted(extraction.detect_numeric_truncations(candidates)) == sorted(expected)


def test_find_prefix_repairs_detects_cases():
    raw = "M\norgachov-ilya@yandex.ru\nVilena\n33 @mail.ru"
    pairs = extraction.find_prefix_repairs(raw)
    assert set(pairs) == {
        ("orgachov-ilya@yandex.ru", "morgachov-ilya@yandex.ru"),
        ("33@mail.ru", "vilena33@mail.ru"),
    }


def test_remove_invisibles_strips_zero_width_and_nbsp():
    raw = "a\u00adb\u2011c\u200b\xa0d"
    assert extraction.remove_invisibles(raw) == "abc d"
