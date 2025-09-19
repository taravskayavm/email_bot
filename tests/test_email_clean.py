import pytest

import config
import utils.email_clean as email_clean
from utils.email_clean import (
    dedupe_with_variants,
    extract_emails,
    parse_emails_unified,
    sanitize_email as _sanitize_email,
)


def sanitize_email(value: str, strip_footnote: bool = True) -> str:
    return _sanitize_email(value, strip_footnote)[0]


@pytest.mark.parametrize(
    "source, expected",
    [
        ("(a) anton-belousov0@rambler.ru", ["anton-belousov0@rambler.ru"]),
        (
            "... tsibulnikova2011@yandex.ru> 550 5.7.1 ...",
            ["tsibulnikova2011@yandex.ru"],
        ),
        (
            "словоanton-belousov0@rambler.ru",
            ["anton-belousov0@rambler.ru"],
        ),
        (
            "anton-belousov0@rambler.ru словоanton-belousov0@rambler.ru",
            ["anton-belousov0@rambler.ru"],
        ),
    ],
)
def test_bounce_samples_are_parsed_cleanly(source, expected):
    parsed = parse_emails_unified(source)
    deduped = dedupe_with_variants(parsed)
    assert deduped == expected


def test_footnote_prefix_removed_and_deduped():
    inp = [
        "¹alexandr.pyatnitsin@yandex.ru",
        "alexandr.pyatnitsin@yandex.ru",
    ]
    out = dedupe_with_variants(inp)
    assert out == ["alexandr.pyatnitsin@yandex.ru"]


def test_punct_trim_and_params():
    assert (
        sanitize_email("(e.kuznetsova@alpfederation.ru)")
        == "e.kuznetsova@alpfederation.ru"
    )
    assert (
        sanitize_email("e.rozhkova@alpfederation.ru?subject=Hi")
        == "e.rozhkova@alpfederation.ru"
    )


def test_zero_width_and_nbsp():
    s = "e.kuznetsova\u200b@alpfederation.ru"
    assert sanitize_email(s) == "e.kuznetsova@alpfederation.ru"


def test_extract_and_clean():
    text = "Связь: ¹e.kuznetsova@alpfederation.ru, ②e.rozhkova@alpfederation.ru"
    emails = dedupe_with_variants(extract_emails(text))
    assert emails == ["e.kuznetsova@alpfederation.ru", "e.rozhkova@alpfederation.ru"]


def test_no_concatenation_on_newlines():
    src = "pavelshabalin@mail.ru\n" "ovalov@gmail.com\n"
    got = extract_emails(src)
    assert "pavelshabalin@mail.ru" in got
    assert "ovalov@gmail.com" in got
    assert not any("mail.ruovalov" in x for x in got)


def test_nbsp_and_zwsp_boundaries():
    src = "name@mail.ru\u00a0\n\u200bsecond@ya.ru"
    got = extract_emails(src)
    assert {"name@mail.ru", "second@ya.ru"} <= set(got)


def test_invalid_concatenation_not_accepted():
    assert sanitize_email("mail.ruovalov@gmail.com") == ""


def test_question_mark_anchor_trimming():
    src = "test@site.com?param=1"
    got = extract_emails(src)
    assert "test@site.com" in got
    assert not any("param=" in x for x in got)


def test_deobfuscation_variants(monkeypatch):
    monkeypatch.setattr(config, "OBFUSCATION_ENABLE", True, raising=False)
    monkeypatch.setattr(email_clean, "OBFUSCATION_ENABLE", True, raising=False)
    src = "user [at] site [dot] ru и user собака site точка ru"
    got = extract_emails(src)
    assert {"user@site.ru"} == set(got)


def test_ocr_comma_before_tld():
    src = "foo@bar,ru"
    got = extract_emails(src)
    assert got == ["foo@bar.ru"]


def test_reject_double_dots_and_hyphen_edges():
    assert sanitize_email("user@foo..bar.com") == ""
    assert sanitize_email("user@-foo.com") == ""
    assert sanitize_email("user@foo-.com") == ""


def test_label_length_and_tld_rules():
    long_label = "a" * 64
    assert sanitize_email(f"user@{long_label}.com") == ""
    assert sanitize_email("user@example.c0m") == ""
    assert sanitize_email("user@example." + "a" * 25) == ""
    assert sanitize_email("user@example." + "a" * 24) == ""


def test_non_ascii_local_is_rejected():
    assert sanitize_email("россияalexdru9@gmail.com") == ""


def test_glued_role_prefix_is_rejected():
    src = "Россия: russia1elena@example.com"
    assert parse_emails_unified(src) == []


def test_plain_role_address_filtered():
    src = "Связь: support@support.com"
    cleaned, meta = parse_emails_unified(src, return_meta=True)
    assert cleaned == []
    reasons = {}
    for item in meta["items"]:
        key = item.get("sanitized") or item.get("normalized") or item.get("raw")
        if key:
            reasons[key] = item.get("reason")
    assert reasons.get("support@support.com") == "role-like-prefix"


def test_role_prefix_sanitize_reason():
    addr, reason = _sanitize_email("info@example.com")
    assert addr == ""
    assert reason == "role-like-prefix"
