from datetime import datetime, timedelta, timezone

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
