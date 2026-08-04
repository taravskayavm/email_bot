from __future__ import annotations

from pathlib import Path

import pytest

from emailbot import extraction
from emailbot.dedupe import remove_pdf_left_glue_duplicates
from emailbot.extraction import EmailHit
from emailbot.extraction_pdf import repair_pdf_email_boundaries
from emailbot.messaging_utils import classify_tld
from emailbot.utils.file_email_extractor import extract_emails_from_bytes
from tests.util_factories import make_pdf


def test_pdf_boundary_repair_requires_strong_glue_signal() -> None:
    source = (
        "editor@journal.ruWebsite "
        "info@publisher.ruwww.publisher.ru "
        "author@example.comAddress "
        "press@mail.ruСайт "
        "office@institute.ruhttps://institute.ru "
        "valid@example.museum "
        "mixed@example.RU"
    )
    stats: dict[str, int] = {}

    repaired = repair_pdf_email_boundaries(source, stats)

    assert "editor@journal.ru Website" in repaired
    assert "info@publisher.ru www.publisher.ru" in repaired
    assert "author@example.com Address" in repaired
    assert "press@mail.ru Сайт" in repaired
    assert "office@institute.ru https://institute.ru" in repaired
    assert "valid@example.museum" in repaired
    assert "mixed@example.RU" in repaired
    assert stats["pdf_email_boundaries_repaired"] == 5


def test_pdf_glued_ru_tail_is_extracted_as_domestic(tmp_path: Path) -> None:
    pdf = make_pdf(
        tmp_path / "journal-glued-boundary.pdf",
        [("Contacts: editor@journal.ruWebsite: https://journal.ru", {})],
    )

    hits, stats = extraction.extract_from_pdf(str(pdf))
    emails = {hit.email.lower() for hit in hits}

    assert "editor@journal.ru" in emails
    assert classify_tld("editor@journal.ru") == "domestic"
    assert stats.get("pdf_email_boundaries_repaired") == 1


def test_uploaded_pdf_uses_same_boundary_repair(tmp_path: Path) -> None:
    pytest.importorskip("pdfminer.high_level")
    pdf = make_pdf(
        tmp_path / "uploaded-journal.pdf",
        [("Editorial office: contact@publisher.ruWebsite", {})],
    )

    emails, rejects, warning = extract_emails_from_bytes(
        pdf.read_bytes(), pdf.name
    )

    assert "contact@publisher.ru" in {email.lower() for email in emails}
    assert rejects == {}
    assert warning is None


def test_pdf_boundary_repair_does_not_change_regular_addresses() -> None:
    source = "one@example.ru two@example.com three@example.museum"

    assert repair_pdf_email_boundaries(source) == source


def test_pdf_country_and_author_code_lines_are_not_glued_to_email() -> None:
    source = (
        "Affiliation: Russia\n"
        "galina.zuckerman@gmail.com\n"
        "1\n"
        "fursovav@bk.ru"
    )
    stats: dict[str, int] = {}

    repaired = repair_pdf_email_boundaries(source, stats)

    assert "Russia; galina.zuckerman@gmail.com" in repaired
    assert "1; fursovav@bk.ru" in repaired
    assert stats["pdf_email_left_boundaries_repaired"] == 2


def _pdf_hit(email: str) -> EmailHit:
    return EmailHit(email=email, source_ref="pdf:journal.pdf#page=1", origin="direct_at")


def test_pdf_duplicate_evidence_removes_country_and_author_codes() -> None:
    hits = [
        _pdf_hit("russia.akhutina@mail.ru"),
        _pdf_hit("akhutina@mail.ru"),
        _pdf_hit("russiagskoblo@mail.ru"),
        _pdf_hit("gskoblo@mail.ru"),
        _pdf_hit("russia.1fursovav@bk.ru"),
        _pdf_hit("fursovav@bk.ru"),
        _pdf_hit("p.2.chpavlov@mail.ru"),
        _pdf_hit("chpavlov@mail.ru"),
    ]
    stats: dict[str, int] = {}

    repaired = remove_pdf_left_glue_duplicates(hits, stats)
    emails = {hit.email for hit in repaired}

    assert emails == {
        "akhutina@mail.ru",
        "gskoblo@mail.ru",
        "fursovav@bk.ru",
        "chpavlov@mail.ru",
    }
    assert stats["pdf_country_prefix_duplicates_removed"] == 3
    assert stats["pdf_author_code_duplicates_removed"] == 1


def test_pdf_left_glue_repair_keeps_ambiguous_real_addresses() -> None:
    hits = [
        _pdf_hit("russia@takeda.com"),
        _pdf_hit("russia-sport@mail.ru"),
        _pdf_hit("russia.editor@mail.ru"),
        _pdf_hit("editor@example.com"),
    ]

    repaired = remove_pdf_left_glue_duplicates(hits, {})

    assert [hit.email for hit in repaired] == [hit.email for hit in hits]
