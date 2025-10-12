def test_blocklist_counts_in_prepare_and_reporting(monkeypatch, tmp_path):
    """
    Гарантируем, что адреса из блок-листа:
      - отфильтровываются в prepare_mass_mailing
      - корректно считаются через reporting.count_blocked
    """
    var = tmp_path / "var"
    var.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SENT_LOG_PATH", str(var / "sent_log.csv"))
    monkeypatch.setenv("SYNC_STATE_PATH", str(var / "sync_state.json"))
    monkeypatch.setenv("SEND_HISTORY_SQLITE_PATH", str(var / "send_history.sqlite"))
    monkeypatch.setenv("HISTORY_DB_PATH", str(var / "history.sqlite"))

    # Подготовим блок-лист
    blocked = tmp_path / "blocked_emails.txt"
    blocked.write_text(
        "blocked1@example.com\nBlocked2@Example.com\n", encoding="utf-8"
    )
    monkeypatch.setenv("BLOCKED_LIST_PATH", str(blocked))
    monkeypatch.setenv("BLOCKED_EMAILS_PATH", str(blocked))

    # Импорт после ENV
    from emailbot import messaging, suppress_list
    from emailbot.reporting import count_blocked

    suppress_list.init_blocked(str(blocked))
    suppress_list.refresh_if_changed()

    emails = [
        "ok@domain.com",
        "blocked1@example.com",
        "blocked2@example.com",
        "another@domain.com",
    ]

    ready, blocked_foreign, blocked_invalid, skipped_recent, digest = messaging.prepare_mass_mailing(
        emails, group="grp", chat_id=None, ignore_cooldown=True
    )

    # Готовые к отправке — без заблокированных
    ready_l = [e.lower() for e in ready]
    assert "blocked1@example.com" not in ready_l
    assert "blocked2@example.com" not in ready_l
    assert "ok@domain.com" in ready_l
    assert "another@domain.com" in ready_l

    # В дайджесте отражается количество, отфильтрованных блок-листом
    assert digest.get("skipped_suppress", 0) == 2

    # И счётчик отчётов совпадает
    assert count_blocked(emails) == 2
