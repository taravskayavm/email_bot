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
