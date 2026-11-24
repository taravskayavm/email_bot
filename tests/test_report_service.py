import datetime as dt
import importlib
import sys
import json

import pytest


def test_summarize_day_local_combines_sources(monkeypatch, tmp_path):
    var = tmp_path / "var"
    var.mkdir(parents=True, exist_ok=True)
    sent_log = var / "sent_log.csv"
    send_stats = var / "send_stats.jsonl"

    monkeypatch.setenv("SENT_LOG_PATH", str(sent_log))
    monkeypatch.setenv("SEND_STATS_PATH", str(send_stats))
    monkeypatch.setenv("REPORT_TZ", "Europe/Moscow")

    sent_log.write_text(
        "\n".join(
            [
                "key,email,last_sent_at,source,status",
                "k1,foo@example.com,2024-05-01T10:00:00+03:00,manual,sent",
                "k2,bar@example.com,2024-05-01T23:30:00+03:00,manual,failed",
                "k3,baz@example.com,2024-05-01T12:00:00,manual,success",
                "k4,out@example.com,2024-04-30T23:59:00+03:00,manual,sent",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    send_stats.write_text(
        "\n".join(
            [
                '{"ts": "2024-04-30T21:00:00Z", "status": "success"}',
                '{"ts": "2024-05-01T15:00:00+03:00", "success": false}',
                '{"ts": "2024-05-02T00:10:00+03:00", "status": "success"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    module_name = "emailbot.report_service"
    sys.modules.pop(module_name, None)
    report_service = importlib.import_module(module_name)

    csv_rows = list(report_service._iter_sent_log())
    stats_rows = list(report_service._iter_send_stats())

    assert len(csv_rows) == 4
    assert len(stats_rows) == 3

    ok, err = report_service.summarize_day_local(dt.date(2024, 5, 1))

    assert ok == 3
    assert err == 2


def test_summarize_day_local_prefers_durable_history(monkeypatch, tmp_path):
    var = tmp_path / "var"
    var.mkdir(parents=True, exist_ok=True)
    sent_log = var / "sent_log.csv"
    send_stats = var / "send_stats.jsonl"

    monkeypatch.setenv("SENT_LOG_PATH", str(sent_log))
    monkeypatch.setenv("SEND_STATS_PATH", str(send_stats))
    monkeypatch.setenv("REPORT_TZ", "Europe/Moscow")
    sent_log.write_text(
        "\n".join(
            [
                "key,email,last_sent_at,source,status",
                "k1,one@example.com,2024-05-01T10:00:00+03:00,manual,sent",
                "k2,two@example.com,2024-05-01T11:00:00+03:00,manual,sent",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    send_stats.write_text("", encoding="utf-8")

    module_name = "emailbot.report_service"
    sys.modules.pop(module_name, None)
    report_service = importlib.import_module(module_name)
    monkeypatch.setattr(report_service, "_history_success_count", lambda *_: 4)

    ok, err = report_service.summarize_day_local(dt.date(2024, 5, 1))

    assert ok == 4
    assert err == 0


@pytest.fixture
def delivery_report(monkeypatch, tmp_path):
    module = importlib.import_module("emailbot.report_service")
    monkeypatch.setattr(module, "SENT_LOG_PATH", tmp_path / "sent.csv")
    monkeypatch.setattr(module, "SEND_STATS_PATH", tmp_path / "stats.jsonl")
    monkeypatch.setattr(module, "_history_success_count", lambda *_: 0)
    return module


def test_late_bounce_corrects_original_day_once(delivery_report):
    module = delivery_report
    module.SENT_LOG_PATH.write_text(
        "email,last_sent_at,status,message_id\n"
        "bad@example.com,2026-11-06T08:10:00Z,sent,original-1\n"
        "good@example.com,2026-11-06T08:11:00Z,sent,original-2\n", encoding="utf-8")
    bounce = {"email": "bad@example.com", "status": "bounce", "message_id": "original-1",
              "ts": "2026-11-09T08:15:00Z", "original_sent_at": "2026-11-06T08:10:00Z"}
    module.SEND_STATS_PATH.write_text(
        json.dumps({**bounce, "bounce_id": "first"}) + "\n" +
        json.dumps({**bounce, "bounce_id": "second"}) + "\n", encoding="utf-8")
    assert module.summarize_day_local(dt.date(2026, 11, 6)) == (1, 1)
    assert module.summarize_day_local(dt.date(2026, 11, 9)) == (0, 0)
    assert module.summarize_day_local(dt.date(2026, 11, 6)) == (1, 1)


def test_durable_history_does_not_restore_bounced_success(delivery_report, monkeypatch):
    from emailbot import reporting
    module = delivery_report
    accepted = [
        {"email": "bad@example.com", "ts": "2026-11-06T08:10:00Z", "message_id": "original-1"},
        {"email": "good@example.com", "ts": "2026-11-06T08:11:00Z", "message_id": "original-2"},
    ]
    monkeypatch.setattr(module, "_history_success_count", lambda *_: 2)
    monkeypatch.setattr(reporting, "_iter_history_events", lambda *_: accepted)
    module.SEND_STATS_PATH.write_text(json.dumps({
        "email": "bad@example.com", "status": "bounce", "message_id": "original-1",
        "ts": "2026-11-09T08:15:00Z", "original_sent_at": "2026-11-06T08:10:00Z",
    }) + "\n", encoding="utf-8")
    assert module.summarize_day_local(dt.date(2026, 11, 6)) == (1, 1)


def test_successful_resend_survives_old_bounce(delivery_report):
    module = delivery_report
    rows = [
        {"email": "bad@example.com", "status": "success", "ts": "2026-11-06T08:10:00Z", "message_id": "first"},
        {"email": "bad@example.com", "status": "success", "ts": "2026-11-06T09:10:00Z", "message_id": "retry"},
        {"email": "bad@example.com", "status": "bounce", "ts": "2026-11-09T08:15:00Z",
         "message_id": "first", "original_sent_at": "2026-11-06T08:10:00Z"},
    ]
    module.SEND_STATS_PATH.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    assert module.summarize_day_local(dt.date(2026, 11, 6)) == (1, 1)


def test_same_success_in_csv_and_jsonl_is_counted_once(delivery_report):
    module = delivery_report
    module.SENT_LOG_PATH.write_text(
        "email,last_sent_at,status\n"
        "bad@example.com,2026-11-06T08:10:00Z,sent\n"
        "good@example.com,2026-11-06T08:11:00Z,sent\n", encoding="utf-8")
    rows = [
        {"email": "bad@example.com", "status": "success", "ts": "2026-11-06T08:10:00Z", "message_id": "first"},
        {"email": "good@example.com", "status": "success", "ts": "2026-11-06T08:11:00Z", "message_id": "second"},
        {"email": "bad@example.com", "status": "bounce", "ts": "2026-11-09T08:15:00Z",
         "message_id": "first", "original_sent_at": "2026-11-06T08:10:00Z"},
    ]
    module.SEND_STATS_PATH.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    assert module.summarize_day_local(dt.date(2026, 11, 6)) == (1, 1)


def test_jsonl_summary_subtracts_late_failure_once(monkeypatch, tmp_path):
    from utils import send_stats
    target = tmp_path / "stats.jsonl"
    monkeypatch.setenv("SEND_STATS_PATH", str(target))
    monkeypatch.setattr(send_stats, "_now_utc", lambda: dt.datetime(2026, 11, 6, 12, tzinfo=dt.timezone.utc))
    rows = [
        {"email": "bad@example.com", "status": "success", "ts": "2026-11-06T08:10:00Z", "message_id": "first"},
        {"email": "good@example.com", "status": "success", "ts": "2026-11-06T08:11:00Z"},
        {"email": "bad@example.com", "status": "bounce", "ts": "2026-11-09T08:15:00Z",
         "message_id": "first", "original_sent_at": "2026-11-06T08:10:00Z"},
    ]
    target.write_text("\n".join(json.dumps(row) for row in rows + [rows[-1]]) + "\n", encoding="utf-8")
    assert send_stats.summarize_today() == {"ok": 1, "err": 1, "success": 1, "error": 1}
