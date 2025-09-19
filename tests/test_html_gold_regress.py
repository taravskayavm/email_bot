import pathlib

from emailbot.extraction import extract_from_html_stream


def test_gold_alpfederation_no_phone_prefix_glue():
    gold = pathlib.Path(__file__).parent / "fixtures" / "gold" / "alpfederation.html"
    data = gold.read_bytes()
    hits, stats = extract_from_html_stream(data, source_ref="gold/alpfederation.html")
    emails = {h.email for h in hits}
    assert "stark_velik@mail.ru" in emails
    assert all(not email.startswith("+7") for email in emails)

