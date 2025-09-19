from datetime import datetime, timedelta, timezone

import importlib

from emailbot import history_service, history_store


def test_mark_and_filter(monkeypatch):
    history_service.ensure_initialized()
    now = datetime.now(timezone.utc).replace(microsecond=0)

    history_service.mark_sent("user@example.com", "grp", "m1", now)
    assert history_service.was_sent_within_days("user@example.com", "grp", 1) is True

    allowed, rejected = history_service.filter_by_days(
        ["user@example.com", "other@example.com"], "grp", 30
    )
    assert allowed == ["other@example.com"]
    assert rejected == ["user@example.com"]

    # Different group is treated independently
    allowed_alt, rejected_alt = history_service.filter_by_days(
        ["user@example.com"], "other", 30
    )
    assert allowed_alt == ["user@example.com"]
    assert rejected_alt == []

    # Expired record no longer blocks
    old_dt = now - timedelta(days=40)
    history_store.record_sent("ancient@example.com", "grp", None, old_dt)
    assert history_service.was_sent_within_days("ancient@example.com", "grp", 30) is False
    assert history_service.was_sent_within_days("ancient@example.com", "grp", 60) is True


def test_get_last_sent(monkeypatch):
    history_service.ensure_initialized()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    earlier = now - timedelta(days=1)

    history_service.mark_sent("rec@example.com", "grp", "m0", earlier)
    history_service.mark_sent("rec@example.com", "grp", "m1", now)

    last = history_service.get_last_sent("rec@example.com", "grp")
    assert last is not None
    assert abs((last - now).total_seconds()) < 1

    info = history_service.get_last_sent_any_group("rec@example.com")
    assert info is not None
    group, last_any = info
    assert group == "grp"
    assert abs((last_any - now).total_seconds()) < 1


def test_register_send_attempt_and_cancel(monkeypatch, tmp_path):
    db = tmp_path / "state.db"
    monkeypatch.setenv("HISTORY_DB_PATH", str(db))
    service = importlib.reload(history_service)

    now = datetime.now(timezone.utc)
    reserved = service.register_send_attempt(
        "user@example.com",
        "grp",
        days=30,
        sent_at=now,
        run_id="run-a",
    )
    assert reserved is not None

    blocked = service.register_send_attempt(
        "user@example.com",
        "grp",
        days=30,
        sent_at=now + timedelta(hours=1),
        run_id="run-b",
    )
    assert blocked is None

    service.cancel_send_attempt("user@example.com", "grp", reserved)

    allowed = service.register_send_attempt(
        "user@example.com",
        "grp",
        days=30,
        sent_at=now + timedelta(hours=2),
        run_id="run-c",
    )
    assert allowed is not None
