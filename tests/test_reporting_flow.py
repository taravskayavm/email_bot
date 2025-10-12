from datetime import datetime, timezone


def test_reporting_counts_after_parse_and_after_send(monkeypatch, tmp_path):
    """Ensure blocked counts stay consistent before and after sending."""

    var = tmp_path / "var"
    var.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SENT_LOG_PATH", str(var / "sent_log.csv"))
    monkeypatch.setenv("SYNC_STATE_PATH", str(var / "sync_state.json"))
    monkeypatch.setenv("SEND_STATS_PATH", str(var / "send_stats.jsonl"))

    blocked = tmp_path / "blocked_emails.txt"
    blocked.write_text("ban1@x.com\nban2@x.com\n", encoding="utf-8")
    monkeypatch.setenv("BLOCKED_LIST_PATH", str(blocked))
    monkeypatch.setenv("BLOCKED_EMAILS_PATH", str(blocked))

    from emailbot import messaging, suppress_list
    from emailbot.reporting import count_blocked
    from utils import rules

    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(blocked))
    monkeypatch.setattr(rules, "BLOCKLIST_PATH", blocked)
    suppress_list.init_blocked(str(blocked))

    emails = ["ok1@x.com", "ban1@x.com", "ok2@y.org", "ban2@x.com"]
    ready, *_rest, digest = messaging.prepare_mass_mailing(emails, ignore_cooldown=True)

    assert digest.get("skipped_suppress", 0) == 2
    assert count_blocked(emails) == 2

    now = datetime(2025, 10, 12, 9, 0, 0, tzinfo=timezone.utc)
    for addr in ready:
        messaging.log_sent_email(
            addr,
            group="grp",
            ts=now,
            subject="S",
            content_hash="H",
        )

    ready2, *_rest2, digest2 = messaging.prepare_mass_mailing(emails, ignore_cooldown=True)

    assert digest2.get("skipped_suppress", 0) == 2
    assert count_blocked(emails) == 2
