from datetime import datetime, timedelta, timezone

from emailbot.cooldown import mark_sent, should_skip_by_cooldown


def test_cooldown_cycle(tmp_path, monkeypatch):
    var = tmp_path / "var"
    var.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SEND_HISTORY_SQLITE_PATH", str(var / "send_history.sqlite"))
    monkeypatch.setenv("HISTORY_DB_PATH", str(var / "history.sqlite"))
    monkeypatch.setenv("SEND_STATS_PATH", str(var / "send_stats.jsonl"))

    now = datetime.now(timezone.utc)
    mark_sent(
        "user@example.com",
        group="grp",
        sent_at=now,
        message_id="mid",
        run_id="run1",
    )

    skip, reason = should_skip_by_cooldown(
        "user@example.com", now=now + timedelta(hours=1), days=180
    )

    assert skip is True
    assert "cooldown" in reason
