import fitz  # PyMuPDF

from emailbot.dedupe import merge_footnote_prefix_variants
from emailbot.extraction import EmailHit, extract_any


def make_hit(email: str, pre: str, source: str = "doc.pdf|1") -> EmailHit:
    return EmailHit(email=email, source_ref=source, origin="direct_at", pre=pre, post="")


def test_trimmed_variant_removed():
    long = make_hit("959536_vorobeva@mail.ru", pre="")
    short = make_hit("59536_vorobeva@mail.ru", pre="9")
    stats = {}
    res = merge_footnote_prefix_variants([long, short], stats)
    assert res == [long]
    assert stats.get("footnote_trimmed_merged") == 1


def test_different_addresses_not_merged():
    a = make_hit("1abc@mail.ru", pre="1")
    b = make_hit("xabc@mail.ru", pre="")
    stats = {}
    res = merge_footnote_prefix_variants([a, b], stats)
    assert {h.email for h in res} == {a.email, b.email}
    assert stats.get("footnote_trimmed_merged", 0) == 0


def _make_pdf(path, text):
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), text, fontsize=12)
    doc.save(str(path))
    doc.close()


def test_pdf_footnote_trimmed_is_merged(tmp_path):
    pdf = tmp_path / "footnote.pdf"
    text = "Контакты: ¹959536_vorobeva@mail.ru и 959536_vorobeva@mail.ru"
    _make_pdf(pdf, text)

    emails, stats = extract_any(str(pdf))
    assert "959536_vorobeva@mail.ru" in emails
    assert "59536_vorobeva@mail.ru" not in emails
    assert stats.get("footnote_trimmed_merged", 0) >= 1

