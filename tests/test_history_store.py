from datetime import datetime, timedelta, timezone

from emailbot import history_store


def test_record_and_query(tmp_path):
    db = tmp_path / "state.db"
    history_store.init_db(db)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    earlier = now - timedelta(days=5)

    history_store.record_sent("User@Example.com", "GroupA", "msg1", earlier)

    assert history_store.was_sent_within("user@example.com", "groupa", 10) is True
    assert history_store.was_sent_within("user@example.com", "groupa", 3) is False

    history_store.record_sent("user@example.com", "GroupA", "msg2", now)
    assert history_store.was_sent_within("USER@example.com", "GROUPA", 1) is True


def test_get_last_sent_returns_latest(tmp_path):
    db = tmp_path / "state.db"
    history_store.init_db(db)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    older = now - timedelta(days=30)

    history_store.record_sent("user@example.com", "grp", "m-old", older)
    history_store.record_sent("user@example.com", "grp", "m-new", now)

    last = history_store.get_last_sent("user@example.com", "grp")
    assert last is not None
    assert abs((last - now).total_seconds()) < 1

    assert history_store.get_last_sent("absent@example.com", "grp") is None
