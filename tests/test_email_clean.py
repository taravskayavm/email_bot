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
