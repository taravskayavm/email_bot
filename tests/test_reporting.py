import json
import logging
import zipfile

from emailbot import extraction
from emailbot.reporting import build_mass_report_text, log_mass_filter_digest


def _records(caplog):
    return [r for r in caplog.records if r.name == "emailbot.digest"]


def test_extract_digest_logging(tmp_path, caplog):
    html = tmp_path / "sample.html"
    html.write_text("<a href='mailto:x@y.ru'>x</a> +m@h.abs", encoding="utf-8")
    with caplog.at_level(logging.INFO, logger="emailbot.digest"):
        extraction.extract_any(str(html))
    recs = _records(caplog)
    assert len(recs) == 1
    data = json.loads(recs[0].message)
    assert data["component"] == "extract"
    for key in ("total_found", "invalid_tld", "elapsed_ms", "entry"):
        assert key in data
    assert "@" not in recs[0].message

    caplog.clear()
    inner = tmp_path / "inner.txt"
    inner.write_text("inner@example.com", encoding="utf-8")
    zip_path = tmp_path / "sample.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(inner, "inner.txt")
    with caplog.at_level(logging.INFO, logger="emailbot.digest"):
        extraction.extract_any(str(zip_path))
    recs = _records(caplog)
    assert len(recs) == 1
    data = json.loads(recs[0].message)
    assert data["component"] == "extract"
    for key in ("total_found", "invalid_tld", "elapsed_ms", "entry"):
        assert key in data
    assert "@" not in recs[0].message


def test_mass_filter_digest_logging(caplog):
    ctx = {
        "input_total": 5,
        "after_suppress": 4,
        "foreign_blocked": 1,
        "after_180d": 3,
        "sent_planned": 2,
        "skipped_by_dup_in_batch": 1,
    }
    with caplog.at_level(logging.INFO, logger="emailbot.digest"):
        log_mass_filter_digest(ctx)
    recs = _records(caplog)
    assert len(recs) == 1
    data = json.loads(recs[0].message)
    assert data["component"] == "mass_filter"
    for key, val in ctx.items():
        assert data[key] == val
    assert "@" not in recs[0].message


def test_build_mass_report_text_ignores_blocked():
    sent_ok = ["a@example.com", "b@example.com"]
    skipped = ["c@example.com"]
    blocked_foreign = ["foreign@example.de"]
    blocked_invalid = ["invalid@example.com"]

    text = build_mass_report_text(sent_ok, skipped, blocked_foreign, blocked_invalid)

    assert "В блок" not in text
    assert "иностранные" not in text
    assert "неработающие" not in text
    assert "✅ Отправлено: 2" in text
    assert "⏳ Пропущены (<180 дней): 1" in text
    assert "• a@example.com" in text
    assert "• c@example.com" in text

