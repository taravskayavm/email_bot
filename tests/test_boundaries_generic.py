import pytest

from utils import email_clean
from utils.email_clean import parse_emails_unified


@pytest.mark.parametrize(
    "prefix",
    [
        "ЛюбоеСлово",
        "Loremipsumdolor",
        "prefixwithoutspace",
        "superlongwordprefix",
        "研究所",
        "قسم",
    ],
)
def test_left_glued_word_becomes_suspect(prefix, monkeypatch):
    """Любое слово (любая письменность) слева без разделителя делает адрес подозрительным."""

    monkeypatch.setenv("STRICT_LEFT_BOUNDARY", "1")
    email_clean.STRICT_LEFT_BOUNDARY = True
    src = f"{prefix}ivanov@mail.ru"
    emails, meta = parse_emails_unified(src, return_meta=True)
    suspects = set(meta.get("suspects") or [])
    assert suspects
    if emails:
        assert set(emails) <= suspects


@pytest.mark.parametrize(
    "suffix",
    [
        "Россия",
        "附加",
        "قسم",
        "слово",
        "研究所",
    ],
)
def test_right_glued_word_becomes_suspect(suffix, monkeypatch):
    """Любое слово справа без разделителя помечает адрес как подозрительный."""

    monkeypatch.setenv("STRICT_LEFT_BOUNDARY", "1")
    email_clean.STRICT_LEFT_BOUNDARY = True
    src = f"ivanov@mail.ru{suffix}"
    emails, meta = parse_emails_unified(src, return_meta=True)
    suspects = set(meta.get("suspects") or [])
    assert suspects
    if emails:
        assert set(emails) <= suspects


def test_orcid_like_and_long_digits_marked_suspect(monkeypatch):
    monkeypatch.setenv("STRICT_LEFT_BOUNDARY", "1")
    email_clean.STRICT_LEFT_BOUNDARY = True
    src = "0000-0002-1234-5678stolbov@mail.ru 79082412863@yandex.ru"
    emails, meta = parse_emails_unified(src, return_meta=True)
    suspects = set(meta.get("suspects") or [])
    assert "0000-0002-1234-5678stolbov@mail.ru" in suspects
    assert "79082412863@yandex.ru" in suspects
    assert set(emails) >= {
        "0000-0002-1234-5678stolbov@mail.ru",
        "79082412863@yandex.ru",
    }
