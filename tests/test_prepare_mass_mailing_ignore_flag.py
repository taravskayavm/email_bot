from datetime import datetime, timedelta, timezone


def test_prepare_mass_mailing_respects_ignore_flag(monkeypatch, tmp_path):
    """
    Проверка, что prepare_mass_mailing:
      - по умолчанию учитывает «180 дней»
      - при ignore_cooldown=True игнорирует ограничение и включает адрес в ready
    """
    var = tmp_path / "var"
    var.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SEND_STATS_PATH", str(var / "send_stats.jsonl"))
    monkeypatch.setenv("SENT_LOG_PATH", str(var / "sent_log.csv"))
    monkeypatch.setenv("SYNC_STATE_PATH", str(var / "sync_state.json"))
    monkeypatch.setenv("SEND_HISTORY_SQLITE_PATH", str(var / "send_history.sqlite"))
    monkeypatch.setenv("HISTORY_DB_PATH", str(var / "history.sqlite"))
    # Пустой блок-лист
    blocked = tmp_path / "blocked_emails.txt"
    blocked.write_text("", encoding="utf-8")

    from emailbot.services import cooldown
    from emailbot import messaging, suppress_list

    suppress_list.init_blocked(blocked)
    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(blocked), raising=False)
    messaging._BLOCK_READY = False

    addr = "user@example.com"
    now = datetime(2025, 10, 12, 8, 0, 0, tzinfo=timezone.utc)

    # Отмечаем недавнюю отправку → правило должно сработать
    cooldown.mark_sent(addr, sent_at=now - timedelta(days=2))

    ready, bf, bi, sr, digest = messaging.prepare_mass_mailing(
        [addr], group="grp", chat_id=None, ignore_cooldown=False
    )
    assert ready == []
    assert digest.get("skipped_180d", 0) == 1

    # А вот с игнором — адрес попадёт в ready
    ready2, bf2, bi2, sr2, digest2 = messaging.prepare_mass_mailing(
        [addr], group="grp", chat_id=None, ignore_cooldown=True
    )
    assert addr in ready2
    assert digest2.get("skipped_180d", 0) == 0


def test_prepare_mass_mailing_allows_ru_and_gmail(monkeypatch, tmp_path):
    var = tmp_path / "var"
    var.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SEND_STATS_PATH", str(var / "send_stats.jsonl"))
    monkeypatch.setenv("SENT_LOG_PATH", str(var / "sent_log.csv"))
    monkeypatch.setenv("SYNC_STATE_PATH", str(var / "sync_state.json"))
    monkeypatch.setenv("FOREIGN_BLOCK", "1")
    monkeypatch.setenv("TLD_ALLOWED", "ru")

    blocked = tmp_path / "blocked_emails.txt"
    blocked.write_text("", encoding="utf-8")

    from emailbot import messaging, suppress_list
    from utils import rules

    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(blocked), raising=False)
    monkeypatch.setattr(rules, "BLOCKLIST_PATH", blocked)
    suppress_list.init_blocked(blocked)
    messaging._BLOCK_READY = False

    emails = ["user@gmail.com", "friend@mail.ru", "foreign@example.de"]
    ready, blocked_foreign, *_rest, digest = messaging.prepare_mass_mailing(
        emails, ignore_cooldown=True
    )

    ready_lower = {addr.lower() for addr in ready}
    assert "user@gmail.com" in ready_lower
    assert "friend@mail.ru" in ready_lower
    assert "foreign@example.de" in {addr.lower() for addr in blocked_foreign}
    assert digest.get("global_excluded") == 0
