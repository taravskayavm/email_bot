import pytest

import config
import utils.email_clean as email_clean
from utils.email_clean import dedupe_keep_original, parse_emails_unified


@pytest.fixture(autouse=True)
def enable_obfuscation(monkeypatch):
    monkeypatch.setattr(config, "OBFUSCATION_ENABLE", True, raising=False)
    monkeypatch.setattr(email_clean, "OBFUSCATION_ENABLE", True, raising=False)


def test_simple_space_delimited():
    src = "a@b.com c@d.com"
    assert parse_emails_unified(src) == ["a@b.com", "c@d.com"]


def test_multiline_and_nbsp_and_zwsp():
    src = "p1@mail.ru\u200b\np2@mail.ru\u00a0p3@mail.ru"
    got = parse_emails_unified(src)
    assert got == ["p1@mail.ru", "p2@mail.ru", "p3@mail.ru"]


def test_fio_glued_with_email():
    src = "ивановсергейamazonka.sambo@list.ru"
    assert parse_emails_unified(src) == ["amazonka.sambo@list.ru"]


def test_deobfuscate_at_dot_ru_and_ru():
    src = "ivan.petrov [at] gmail [dot] com; pavel(собака)yandex(точка)ru"
    got = parse_emails_unified(src)
    assert "ivan.petrov@gmail.com" in got
    assert "pavel@yandex.ru" in got


def test_ocr_comma_before_tld():
    src = "user@mail,ru and other"
    assert parse_emails_unified(src) == ["user@mail.ru"]


def test_provider_dedupe_gmail_dots_plus():
    src = "ivan.petrov+tag@gmail.com ivanpetrov@gmail.com"
    got = dedupe_keep_original(parse_emails_unified(src))
    assert got == ["ivan.petrov+tag@gmail.com"]


def test_provider_dedupe_yandex_plus():
    src = "pavel+news@yandex.ru pavel@yandex.ru"
    got = dedupe_keep_original(parse_emails_unified(src))
    assert got == ["pavel+news@yandex.ru"]


def test_provider_dedupe_mailru_plus():
    src = "name+abc@mail.ru name@mail.ru"
    got = dedupe_keep_original(parse_emails_unified(src))
    assert got == ["name+abc@mail.ru"]


def test_unicode_domain_punycode_kept_correct():
    src = "test@тест.рф"
    emails, meta = parse_emails_unified(src, return_meta=True)
    assert emails == []
    assert meta["items"][0]["reason"] == "tld-not-allowed"


@pytest.mark.parametrize("flag", ["0", "1"])
def test_debug_flag_does_not_change_output(monkeypatch, flag):
    monkeypatch.setenv("EMAIL_PARSE_DEBUG", flag)
    src = "a@b.com, c@d.com"
    got = parse_emails_unified(src)
    assert got == ["a@b.com", "c@d.com"]


def test_numeric_only_local_from_footnote_is_rejected():
    # типичный артефакт: «¹» в PDF превратился в "1" и «приклеился» к домену
    src = "1@gmail.com, author.name@uni.ru, 0@yandex.ru"
    got, meta = parse_emails_unified(src, return_meta=True)
    # должны остаться только валидные адреса, а «1@…» и «0@…» уйдут с причиной looks-like-footnote
    assert got == ["author.name@uni.ru"]
    drop_reasons = {item["raw"]: item["reason"] for item in meta["items"]}
    assert drop_reasons.get("1@gmail.com") == "looks-like-footnote"
    assert drop_reasons.get("0@yandex.ru") == "looks-like-footnote"
