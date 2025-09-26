from datetime import datetime

from emailbot import history_store
from emailbot.edit_service import apply_edits, clear_edits, list_edits, save_edit


def test_edit_service_roundtrip(tmp_path):
    db_path = tmp_path / "state.db"
    history_store.init_db(db_path)
    chat_id = 90210
    clear_edits(chat_id)

    save_edit(chat_id, "old@example.com", "new@example.com", when=datetime(2024, 1, 1))

    updated = apply_edits(["old@example.com", "other@example.com"], chat_id)
    assert updated == ["new@example.com", "other@example.com"]

    rows = list_edits(chat_id)
    assert len(rows) == 1
    assert rows[0][0] == "old@example.com"
    assert rows[0][1] == "new@example.com"

    clear_edits(chat_id)
    assert list_edits(chat_id) == []


def test_apply_edits_handles_canonicalization_and_drop(tmp_path):
    db_path = tmp_path / "state.db"
    history_store.init_db(db_path)
    chat_id = 111
    clear_edits(chat_id)

    save_edit(chat_id, "User (at) Example.com", "Clean+tag@Example.COM")
    save_edit(chat_id, "remove.me@example.com", "-", when=datetime(2024, 2, 2))

    source = [
        " user@example.com ",
        "REMOVE.ME@example.com",
        "Other@Example.com",
        "USER@example.com",
    ]

    updated = apply_edits(source, chat_id)

    assert updated == [
        "Clean+tag@example.com",
        "Other@example.com",
    ]
