from __future__ import annotations

from pathlib import Path

from emailbot import extraction
from tests.util_factories import make_pdf


def test_pdf_with_many_direct_addresses_does_not_invent_layout_emails(
    tmp_path: Path,
) -> None:
    expected = [
        "author.one@gmail.com",
        "author.two@yahoo.com",
        "author.three@outlook.com",
        "author4@mail.ru",
        "author5@yandex.ru",
        "author6@institute.ru",
        "author7@journal.ru",
        "author8@university.ru",
    ]
    text = "\n".join(
        [
            "Russia",
            expected[0],
            "Corresponding author",
            *expected[1:],
            "Website: https://journal.ru",
        ]
    )
    pdf = make_pdf(tmp_path / "journal-many-direct.pdf", [(text, {})])

    hits, stats = extraction.extract_from_pdf(str(pdf))

    assert {hit.email.lower() for hit in hits} == set(expected)
    assert stats["pdf_fast_documents"] == 1
    assert stats["pdf_fast_hits"] == len(expected)

    stream_hits, stream_stats = extraction.extract_from_pdf_stream(
        pdf.read_bytes(), source_ref="zip:test.zip|journal-many-direct.pdf"
    )

    assert {hit.email.lower() for hit in stream_hits} == set(expected)
    assert stream_stats["pdf_fast_documents"] == 1
    assert stream_stats["pdf_fast_hits"] == len(expected)
