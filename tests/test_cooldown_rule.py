from datetime import datetime, timedelta, timezone

def test_cooldown_should_skip_and_ignore(monkeypatch, tmp_path):
    """
    Проверяем правило «180 дней» и флаг игнора при ручной рассылке:
      1) should_skip_by_cooldown → True, если письмо было недавно
      2) prepare_mass_mailing(..., ignore_cooldown=True) пропускает ограничение
    """
    # Готовим тестовую "var" папку, чтобы не трогать прод-данные
    var = tmp_path / "var"
    var.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SEND_STATS_PATH", str(var / "send_stats.jsonl"))
    monkeypatch.setenv("SENT_LOG_PATH", str(var / "sent_log.csv"))
    monkeypatch.setenv("SYNC_STATE_PATH", str(var / "sync_state.json"))
    monkeypatch.setenv("SEND_HISTORY_SQLITE_PATH", str(var / "send_history.sqlite"))
    monkeypatch.setenv("HISTORY_DB_PATH", str(var / "history.sqlite"))
    # Блок-лист укажем на пустой временный файл
    blocked = tmp_path / "blocked_emails.txt"
    blocked.write_text("", encoding="utf-8")
    monkeypatch.setenv("BLOCKED_LIST_PATH", str(blocked))
    monkeypatch.setenv("BLOCKED_EMAILS_PATH", str(blocked))

    # Импортируем после настройки ENV, чтобы модули подхватили правильные пути
    from emailbot.services import cooldown
    from emailbot import messaging, suppress_list

    suppress_list.init_blocked(str(blocked))
    suppress_list.refresh_if_changed()

    # Исходные данные
    now = datetime(2025, 10, 12, 12, 0, 0, tzinfo=timezone.utc)
    addr = "test.user+alias@gmail.com"

    # 1) До отметки: не должно скипать
    skip, reason = cooldown.should_skip_by_cooldown(addr, now=now, days=180)
    assert skip is False
    assert reason == ""

    # 2) Отмечаем отправку «вчера» → должно скипать
    cooldown.mark_sent(addr, group=None, sent_at=now - timedelta(days=1))
    skip, reason = cooldown.should_skip_by_cooldown(addr, now=now, days=180)
    assert skip is True
    assert "cooldown<" in reason and "last=" in reason

    # 3) В подготовке ручной рассылки с ignore_cooldown=False адрес попадет в skipped_180d
    ready, blocked_foreign, blocked_invalid, skipped_recent, digest = messaging.prepare_mass_mailing(
        [addr], group="test", chat_id=None, ignore_cooldown=False
    )
    assert ready == []  # отфильтрован правилом 180 дней
    assert len(skipped_recent) == 1
    assert digest.get("skipped_180d", 0) == 1

    # 4) А с ignore_cooldown=True адрес попадет в отправку
    ready2, bf2, bi2, sr2, digest2 = messaging.prepare_mass_mailing(
        [addr], group="test", chat_id=None, ignore_cooldown=True
    )
    assert addr.lower() in [a.lower() for a in ready2]
    assert digest2.get("skipped_180d", 0) == 0


def test_180_days_including_today_window(monkeypatch, tmp_path):
    """
    Явно проверяем, что окно считает «включая сегодняшний день».
    """
    from emailbot.services import cooldown

    var = tmp_path / "var2"
    var.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SEND_STATS_PATH", str(var / "send_stats.jsonl"))
    monkeypatch.setenv("SEND_HISTORY_SQLITE_PATH", str(var / "send_history.sqlite"))
    monkeypatch.setenv("HISTORY_DB_PATH", str(var / "history.sqlite"))

    now = datetime(2025, 10, 12, 0, 0, 0, tzinfo=timezone.utc)
    addr = "sample@example.com"

    # отметили отправку 179 дней назад → не скипаем
    cooldown.mark_sent(addr, sent_at=now - timedelta(days=179))
    skip, _ = cooldown.should_skip_by_cooldown(addr, now=now, days=180)
    assert skip is False

    # отметили отправку ровно 180 дней назад → считаем "в пределах окна" → скипаем
    cooldown.mark_sent(addr, sent_at=now - timedelta(days=180))
    skip2, _ = cooldown.should_skip_by_cooldown(addr, now=now, days=180)
    assert skip2 is True
