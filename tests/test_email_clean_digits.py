import os
import pytest

from utils.email_clean import sanitize_email
from utils.email_clean import parse_emails_unified  # если другой путь — поправить импорт


def test_sanitize_email_keeps_leading_digit_and_dash():
    original = "0-ju@mail.ru"
    assert sanitize_email(original) == original


def test_unified_parser_keeps_leading_digit_and_dash():
    text = "Контакты: 0-ju@mail.ru, а также test.user+tag@yandex.ru"
    emails = parse_emails_unified(text)
    assert "0-ju@mail.ru" in emails


def test_sanitize_email_strips_superscript_footnote_only():
    # ¹alex@mail.ru -> alex@mail.ru (надстрочная 1)
    superscript = "\u00B9alex@mail.ru"
    assert sanitize_email(superscript) == "alex@mail.ru"
