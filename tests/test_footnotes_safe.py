import pytest

from emailbot.footnotes import remove_footnotes_safe


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Россияa nik@example.com", "Россия nik@example.com"),
        ("Смотритеa nik@example.com", "Смотрите nik@example.com"),
        ("Contact: nik@example.com", "Contact: nik@example.com"),
    ],
)
def test_remove_footnotes_keeps_emails(raw, expected):
    assert remove_footnotes_safe(raw) == expected


def test_email_with_digits_intact():
    text = "Напишите на 123ivan@example.com"
    assert remove_footnotes_safe(text) == text
