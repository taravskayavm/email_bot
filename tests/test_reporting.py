import json
import logging
import zipfile

from emailbot import extraction
from emailbot.reporting import (
    build_mass_report_text,
    log_mass_filter_digest,
    render_summary,
)


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
    for key in (
        "total_found",
        "invalid_tld",
        "elapsed_ms",
        "entry",
        "left_guard_skips",
        "prefix_expanded",
        "phone_prefix_stripped",
        "footnote_singletons_repaired",
    ):
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
    for key in (
        "total_found",
        "invalid_tld",
        "elapsed_ms",
        "entry",
        "left_guard_skips",
        "prefix_expanded",
        "phone_prefix_stripped",
        "footnote_singletons_repaired",
    ):
        assert key in data
    assert "@" not in recs[0].message


def test_mass_filter_digest_logging(caplog):
    ctx = {
        "input_total": 5,
        "after_suppress": 4,
        "foreign_blocked": 1,
        "ready_after_cooldown": 3,
        "sent_planned": 2,
        "removed_duplicates_in_batch": 1,
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


def test_build_mass_report_text_counts_only():
    sent_ok = ["a@example.com", "b@example.com"]
    skipped = ["c@example.com"]
    blocked_foreign = ["foreign@example.de"]
    blocked_invalid = ["invalid@example.com"]

    text = build_mass_report_text(sent_ok, skipped, blocked_foreign, blocked_invalid)

    assert "@" not in text
    assert "✉️ Рассылка завершена." in text
    assert "📦 В очереди было: 5" in text
    assert "✅ Успешно отправлено: 2" in text
    assert "⏳ Пропущены (по правилу «180 дней»): 1" in text
    assert "🚫 В стоп-листе: 1" in text
    assert "🌍 Иностранные (отложены): 1" in text


def test_render_summary_always_shows_blocked_line():
    stats = {"total_found": 5, "unique_after_cleanup": 3, "blocked_total": 0}

    summary = render_summary(stats)

    assert "📦 К отправке: 3" in summary
    assert "🚫 Из стоп-листа: 0" in summary

