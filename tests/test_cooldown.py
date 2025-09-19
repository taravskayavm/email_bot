import importlib
import json
import re
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture()
def cooldown_module(monkeypatch, tmp_path):
    stats_path = tmp_path / "send_stats.jsonl"
    sqlite_path = tmp_path / "cooldown.sqlite"
    monkeypatch.setenv("SEND_STATS_PATH", str(stats_path))
    monkeypatch.setenv("APPEND_TO_SENT", "0")
    monkeypatch.setenv("SEND_HISTORY_SQLITE_PATH", str(sqlite_path))
    module = importlib.import_module("emailbot.services.cooldown")
    return importlib.reload(module), stats_path


def test_normalize_email_for_key_variants(cooldown_module):
    cooldown, _ = cooldown_module
    assert cooldown.normalize_email_for_key("Foo.Bar+tag@GMAIL.com") == "foobar@gmail.com"
    assert cooldown.normalize_email_for_key("USER@Example.COM") == "user@example.com"
    idna = cooldown.normalize_email_for_key("пример@почта.рф")
    assert idna.split("@", 1)[1] == "xn--oa-hmcny.xn--p-eub"


def test_should_skip_by_cooldown_absent(cooldown_module):
    cooldown, _ = cooldown_module
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    skip, reason = cooldown.should_skip_by_cooldown("fresh@example.com", now=now, days=180)
    assert skip is False
    assert reason == ""


def test_should_skip_by_cooldown_recent(cooldown_module):
    cooldown, stats_path = cooldown_module
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    recent = now - timedelta(days=3)
    stats_path.write_text(
        json.dumps(
            {
                "email": "Test.User+foo@gmail.com",
                "ts": recent.isoformat().replace("+00:00", "Z"),
            }
        )
        + "\n"
    )
    skip, reason = cooldown.should_skip_by_cooldown("testuser@gmail.com", now=now, days=180)
    assert skip is True
    assert re.search(r"remain≈\d+d \d+h \d+m", reason)


def test_should_allow_after_window(cooldown_module):
    cooldown, stats_path = cooldown_module
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    older = now - timedelta(days=181)
    stats_path.write_text(
        json.dumps(
            {
                "email": "пример@почта.рф",
                "ts": older.isoformat().replace("+00:00", "Z"),
            }
        )
        + "\n"
    )
    skip, reason = cooldown.should_skip_by_cooldown("пример@почта.рф", now=now, days=180)
    assert skip is False
    assert reason == ""


def test_should_use_history_registry(cooldown_module):
    cooldown, _ = cooldown_module
    from emailbot import history_service

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    history_service.ensure_initialized()
    history_service.mark_sent("history@example.com", "grp", "msg", now - timedelta(days=5))

    skip, reason = cooldown.should_skip_by_cooldown("history@example.com", now=now, days=180)
    assert skip is True
    assert "source=history" in reason


def test_sqlite_cache_mark_and_check(cooldown_module):
    cooldown, _ = cooldown_module
    now = datetime(2024, 1, 10, tzinfo=timezone.utc)
    earlier = now - timedelta(days=2)
    cooldown.mark_sent("User@Example.com", sent_at=earlier)
    assert cooldown.was_sent_recently("user@example.com", now=now, days=3) is True
    assert cooldown.was_sent_recently("user@example.com", now=now, days=1) is False
