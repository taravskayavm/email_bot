import datetime as dt
import importlib
import sys


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
