from pathlib import Path

from utils.blocked_store import BlockedStore
from utils.email_normalize import normalize_email


def test_normalize_email_idna_confusables():
    assert normalize_email("Тест <Ivаn.Petroв@пример.рф>")
    assert normalize_email("ivan.petrov@xn--e1afmkfd.xn--p1ai").endswith(".xn--p1ai")


def test_block_store_add_and_contains(tmp_path: Path):
    path = tmp_path / "blocked_emails.txt"
    store = BlockedStore(path)
    added = store.add_many(["User@EXAMPLE.com", "user@example.com", "bad@@example", ""])
    assert added == 1
    assert store.contains("user@example.com")
    assert store.contains("USER@EXAMPLE.COM")
