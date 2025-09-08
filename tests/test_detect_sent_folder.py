import types
from pathlib import Path

from emailbot.messaging_utils import detect_sent_folder, SENT_CACHE_FILE


class DummyImap:
    def __init__(self, mailboxes):
        # формат ответа imap.list(): ('OK', [b'(\\HasNoChildren \\Sent) "/" "Sent"', ...])
        self._mailboxes = mailboxes

    def list(self):
        return "OK", [mb.encode("utf-8") for mb in self._mailboxes]


def test_detect_sent_folder_prefers_flagged_sent(tmp_path, monkeypatch):
    # перенаправим кэш в временную директорию
    monkeypatch.setattr("emailbot.messaging_utils.SENT_CACHE_FILE", tmp_path / "imap_sent_folder.txt")
    imap = DummyImap(
        [
            '(\\HasNoChildren) "/" "INBOX"',
            '(\\HasNoChildren \\Sent) "/" "Sent"',
            '(\\HasNoChildren) "/" "Отправленные"',
        ]
    )
    sent = detect_sent_folder(imap)
    assert sent == "Sent"
    # и кэш должен сохраниться
    assert (tmp_path / "imap_sent_folder.txt").read_text(encoding="utf-8").strip() == "Sent"


def test_detect_sent_folder_localized_ru(tmp_path, monkeypatch):
    monkeypatch.setattr("emailbot.messaging_utils.SENT_CACHE_FILE", tmp_path / "imap_sent_folder.txt")
    imap = DummyImap(
        [
            '(\\HasNoChildren) "/" "INBOX"',
            '(\\HasNoChildren) "/" "Отправленные"',
        ]
    )
    sent = detect_sent_folder(imap)
    assert sent == "Отправленные"


def test_detect_sent_folder_uses_cache(tmp_path, monkeypatch):
    cache = tmp_path / "imap_sent_folder.txt"
    cache.write_text("Sent", encoding="utf-8")
    monkeypatch.setattr("emailbot.messaging_utils.SENT_CACHE_FILE", cache)
    # list не должен вызываться, вернётся кэш
    imap = DummyImap([])
    sent = detect_sent_folder(imap)
    assert sent == "Sent"
