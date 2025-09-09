from utils.email_clean import sanitize_email, dedupe_with_variants, extract_emails


def test_footnote_prefix_removed_and_deduped():
    inp = [
        "55alexandr.pyatnitsin@yandex.ru",
        "alexandr.pyatnitsin@yandex.ru",
    ]
    out = dedupe_with_variants(inp)
    assert out == ["alexandr.pyatnitsin@yandex.ru"]


def test_punct_trim_and_params():
    assert sanitize_email("(e.kuznetsova@alpfederation.ru)") == "e.kuznetsova@alpfederation.ru"
    assert sanitize_email('e.rozhkova@alpfederation.ru?subject=Hi') == "e.rozhkova@alpfederation.ru"


def test_zero_width_and_nbsp():
    s = "e.kuznetsova\u200b@alpfederation.ru"
    assert sanitize_email(s) == "e.kuznetsova@alpfederation.ru"


def test_extract_and_clean():
    text = "Связь: ¹e.kuznetsova@alpfederation.ru, ②e.rozhkova@alpfederation.ru"
    emails = dedupe_with_variants(extract_emails(text))
    assert emails == ["e.kuznetsova@alpfederation.ru", "e.rozhkova@alpfederation.ru"]


def test_no_concatenation_on_newlines():
    src = ("pavelshabalin@mail.ru\n"
           "ovalov@gmail.com\n")
    got = extract_emails(src)
    assert "pavelshabalin@mail.ru" in got
    assert "ovalov@gmail.com" in got
    assert not any("mail.ruovalov" in x for x in got)


def test_nbsp_and_zwsp_boundaries():
    src = "name@mail.ru\u00A0\n\u200bsecond@ya.ru"
    got = extract_emails(src)
    assert {"name@mail.ru", "second@ya.ru"} <= set(got)


def test_invalid_concatenation_not_accepted():
    assert sanitize_email("mail.ruovalov@gmail.com") == ""


def test_question_mark_anchor_trimming():
    src = "test@site.com?param=1"
    got = extract_emails(src)
    assert "test@site.com" in got
    assert not any("param=" in x for x in got)


def test_deobfuscation_variants():
    src = "user [at] site [dot] ru и user собака site точка ru"
    got = extract_emails(src)
    assert {"user@site.ru"} == set(got)


def test_ocr_comma_before_tld():
    src = "foo@bar,ru"
    got = extract_emails(src)
    assert got == ["foo@bar.ru"]
