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
    assert history_store.was_sent_within_any_group("user@example.com", 10) is True


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


def test_last_send_any_group(tmp_path):
    db = tmp_path / "state.db"
    history_store.init_db(db)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    past = now - timedelta(days=2)

    history_store.record_sent("user@example.com", "grp1", "m1", past)
    history_store.record_sent("user@example.com", "grp2", "m2", now)

    info = history_store.last_send_any_group("user@example.com")
    assert info is not None
    group, last = info
    assert group == "grp2"
    assert abs((last - now).total_seconds()) < 1

    assert history_store.last_send_any_group("absent@example.com") is None


def test_try_reserve_send_blocks_within_window(tmp_path):
    db = tmp_path / "state.db"
    history_store.init_db(db)
    now = datetime.now(timezone.utc)

    first = history_store.try_reserve_send(
        "user@example.com",
        "grp",
        now,
        cooldown=timedelta(days=180),
        run_id="run-1",
    )
    assert first is True

    blocked = history_store.try_reserve_send(
        "user@example.com",
        "grp",
        now + timedelta(hours=1),
        cooldown=timedelta(days=180),
        run_id="run-2",
    )
    assert blocked is False

    history_store.delete_send_record("user@example.com", "grp", now)

    allowed_again = history_store.try_reserve_send(
        "user@example.com",
        "grp",
        now + timedelta(hours=2),
        cooldown=timedelta(days=180),
        run_id="run-3",
    )
    assert allowed_again is True
