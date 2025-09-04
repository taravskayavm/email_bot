import pytest
from emailbot.extraction import smart_extract_emails


def test_numeric_prefix_trimmed():
    assert smart_extract_emails(" 1john@example.com") == ["john@example.com"]


def test_numeric_prefix_with_marker_trimmed():
    assert smart_extract_emails("1)john@example.com") == ["john@example.com"]


def test_letter_prefix_with_list_context_trimmed():
    assert smart_extract_emails("a)john@example.com") == ["john@example.com"]


def test_letter_prefix_without_context_kept():
    assert smart_extract_emails(" ajohn@example.com") == ["ajohn@example.com"]


def test_letter_prefix_multi_mode_trimmed():
    text = " afoo@example.com\n bbar@example.com\n cqux@example.com\n ajane@example.com"
    assert smart_extract_emails(text) == [
        "foo@example.com",
        "bar@example.com",
        "qux@example.com",
        "jane@example.com",
    ]


def test_letter_b_no_multi_mode_kept():
    assert smart_extract_emails(" bjane@example.com") == ["bjane@example.com"]


def test_letter_c_with_list_context_trimmed():
    assert smart_extract_emails("c)joe@example.com") == ["joe@example.com"]


def test_rfc_chars_preserved_after_trim():
    assert smart_extract_emails("1x.y+z@example.com") == ["x.y+z@example.com"]
