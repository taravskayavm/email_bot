from __future__ import annotations
from pathlib import Path
import emailbot.extraction as extraction
from tests.util_factories import make_pdf


def _emails(hits):
    return {h.email.lower() for h in hits}


def test_pdf_superscript_footnotes(tmp_path: Path):
    pdf = make_pdf(tmp_path / "footnotes.pdf", [
        ("\u00b9", {"superscript": True}), ("96soul@mail.ru, ", {}),
        ("\u00b9", {"superscript": True}), ("alex@yandex.ru, ", {}),
        ("20yaik11@mail.ru", {}),
    ])

    hits, stats = extraction.extract_any(str(pdf), _return_hits=True)
    emails = _emails(hits)

    assert "96soul@mail.ru" in emails
    assert "alex@yandex.ru" in emails
    assert "20yaik11@mail.ru" in emails

    assert int(stats.get("footnote_singletons_repaired", 0)) >= 0
