import importlib
import re
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture()
def cooldown_module(monkeypatch, tmp_path):
    sqlite_path = tmp_path / "cooldown.sqlite"
    history_path = tmp_path / "history.sqlite"
    monkeypatch.setenv("APPEND_TO_SENT", "0")
    monkeypatch.setenv("SEND_HISTORY_SQLITE_PATH", str(sqlite_path))
    monkeypatch.setenv("HISTORY_DB_PATH", str(history_path))
    monkeypatch.setenv("REPORT_TZ", "UTC")
    module = importlib.import_module("emailbot.services.cooldown")
    return importlib.reload(module)


def test_normalize_email_for_key_variants(cooldown_module):
    cooldown = cooldown_module
    assert cooldown.normalize_email_for_key("Foo.Bar+tag@GMAIL.com") == "foobar@gmail.com"
    assert cooldown.normalize_email_for_key("USER@Example.COM") == "user@example.com"
    idna = cooldown.normalize_email_for_key("пример@почта.рф")
    assert idna.split("@", 1)[1] == "xn--oa-hmcny.xn--p-eub"


def test_should_skip_by_cooldown_absent(cooldown_module):
    cooldown = cooldown_module
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    skip, reason = cooldown.should_skip_by_cooldown("fresh@example.com", now=now, days=180)
    assert skip is False
    assert reason == ""


def test_should_skip_by_cooldown_recent(cooldown_module):
    cooldown = cooldown_module
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    recent = now - timedelta(days=3)
    cooldown.mark_sent("Test.User+foo@gmail.com", sent_at=recent)
    skip, reason = cooldown.should_skip_by_cooldown("testuser@gmail.com", now=now, days=180)
    assert skip is True
    assert re.search(r"remain≈\d+d \d+h \d+m", reason)
    assert "source=history" in reason


def test_should_skip_when_same_day(cooldown_module):
    cooldown = cooldown_module
    now = datetime(2024, 1, 1, 12, tzinfo=timezone.utc)
    earlier_same_day = now - timedelta(hours=3)
    cooldown.mark_sent("today@example.com", sent_at=earlier_same_day)

    skip, reason = cooldown.should_skip_by_cooldown("today@example.com", now=now, days=180)
    assert skip is True
    assert "cooldown<180d" in reason
    assert "source=history" in reason


def test_should_allow_after_window(cooldown_module):
    cooldown = cooldown_module
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    older = now - timedelta(days=181)
    cooldown.mark_sent("пример@почта.рф", sent_at=older)
    skip, reason = cooldown.should_skip_by_cooldown("пример@почта.рф", now=now, days=180)
    assert skip is False
    assert reason == ""


def test_should_use_history_registry(cooldown_module):
    cooldown = cooldown_module
    from emailbot import history_service

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    history_service.ensure_initialized()
    history_service.mark_sent("history@example.com", "grp", "msg", now - timedelta(days=5))

    skip, reason = cooldown.should_skip_by_cooldown("history@example.com", now=now, days=180)
    assert skip is True
    assert "source=history" in reason


def test_sqlite_cache_mark_and_check(cooldown_module):
    cooldown = cooldown_module
    now = datetime(2024, 1, 10, tzinfo=timezone.utc)
    earlier = now - timedelta(days=2)
    cooldown.mark_sent("User@Example.com", sent_at=earlier)
    assert cooldown.was_sent_recently("user@example.com", now=now, days=3) is True
    assert cooldown.was_sent_recently("user@example.com", now=now, days=1) is False
