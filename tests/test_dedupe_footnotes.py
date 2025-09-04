import fitz  # PyMuPDF

from emailbot.dedupe import merge_footnote_prefix_variants, repair_footnote_singletons
from emailbot.extraction import EmailHit, extract_any


def make_hit(email: str, pre: str, source: str = "doc.pdf|1") -> EmailHit:
    return EmailHit(email=email, source_ref=source, origin="direct_at", pre=pre, post="")


def test_trimmed_variant_removed():
    long = make_hit("959536_vorobeva@mail.ru", pre="")
    short = make_hit("59536_vorobeva@mail.ru", pre="9")
    stats = {}
    res = merge_footnote_prefix_variants([long, short], stats)
    assert res == [long]
    assert stats.get("footnote_pairs_merged") == 1


def test_different_addresses_not_merged():
    a = make_hit("1abc@mail.ru", pre="1")
    b = make_hit("xabc@mail.ru", pre="")
    stats = {}
    res = merge_footnote_prefix_variants([a, b], stats)
    assert {h.email for h in res} == {a.email, b.email}
    assert stats.get("footnote_pairs_merged", 0) == 0


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
    assert stats.get("footnote_pairs_merged", 0) >= 1


def test_singleton_digit_repaired():
    h = make_hit("1dergal@yandex.ru", pre="¹", source="pdf:doc.pdf")
    res, fixed = repair_footnote_singletons([h])
    assert [x.email for x in res] == ["dergal@yandex.ru"]
    assert fixed == 1


def test_singleton_two_digits_repaired():
    h = make_hit("196soul@mail.ru", pre="¹", source="pdf:doc.pdf")
    res, fixed = repair_footnote_singletons([h])
    assert [x.email for x in res] == ["96soul@mail.ru"]
    assert fixed == 1


def test_singleton_without_superscript_not_repaired():
    h = make_hit("1dergal@yandex.ru", pre="1", source="pdf:doc.pdf")
    res, fixed = repair_footnote_singletons([h])
    assert [x.email for x in res] == ["1dergal@yandex.ru"]
    assert fixed == 0


def test_real_address_not_repaired():
    h = make_hit("20yaik11@mail.ru", pre="", source="pdf:doc.pdf")
    res, fixed = repair_footnote_singletons([h])
    assert [x.email for x in res] == ["20yaik11@mail.ru"]
    assert fixed == 0

