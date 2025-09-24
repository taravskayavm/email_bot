from emailbot.messaging_utils import detect_sent_folder


class DummyImap:
    def __init__(self, mailboxes, selectable=None):
        # формат ответа imap.list(): ('OK', [b'(\\HasNoChildren \\Sent) "/" "Sent"', ...])
        self._mailboxes = mailboxes
        self._selectable = set(selectable or [])
        self.selected = []

    def list(self):
        return "OK", self._mailboxes

    def select(self, mailbox, readonly=False):
        self.selected.append(mailbox)
        if mailbox in self._selectable:
            return "OK", [b""]
        return "NO", [b""]


def test_detect_sent_folder_prefers_flagged_sent(tmp_path, monkeypatch):
    # перенаправим кэш в временную директорию
    monkeypatch.setattr(
        "emailbot.messaging_utils.SENT_CACHE_FILE", tmp_path / "imap_sent_folder.txt"
    )
    monkeypatch.delenv("SENT_MAILBOX", raising=False)
    imap = DummyImap(
        [
            b'(\\HasNoChildren) "/" "INBOX"',
            b'(\\HasNoChildren \\Sent) "/" "Sent"',
            b'(\\HasNoChildren) "/" "&BB4EQgQ,BEAEMAQyBDsENQQ9BD0ESwQ1-"',
        ],
        selectable={"Sent", "Отправленные"},
    )
    sent = detect_sent_folder(imap)
    assert sent == "Sent"
    # и кэш должен сохраниться
    assert (tmp_path / "imap_sent_folder.txt").read_text(
        encoding="utf-8"
    ).strip() == "Sent"


def test_detect_sent_folder_localized_ru(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "emailbot.messaging_utils.SENT_CACHE_FILE", tmp_path / "imap_sent_folder.txt"
    )
    monkeypatch.delenv("SENT_MAILBOX", raising=False)
    imap = DummyImap(
        [
            b'(\\HasNoChildren) "/" "INBOX"',
            b'(\\HasNoChildren) "/" "&BB4EQgQ,BEAEMAQyBDsENQQ9BD0ESwQ1-"',
        ],
        selectable={"Отправленные"},
    )
    sent = detect_sent_folder(imap)
    assert sent == "Отправленные"


def test_detect_sent_folder_uses_cache(tmp_path, monkeypatch):
    cache = tmp_path / "imap_sent_folder.txt"
    cache.write_text("Sent", encoding="utf-8")
    monkeypatch.setattr("emailbot.messaging_utils.SENT_CACHE_FILE", cache)
    monkeypatch.delenv("SENT_MAILBOX", raising=False)
    # list не должен вызываться, вернётся кэш
    imap = DummyImap([], selectable={"Sent"})
    sent = detect_sent_folder(imap)
    assert sent == "Sent"


def test_detect_sent_folder_prefers_env_override(tmp_path, monkeypatch):
    cache = tmp_path / "imap_sent_folder.txt"
    monkeypatch.setattr("emailbot.messaging_utils.SENT_CACHE_FILE", cache)
    monkeypatch.setenv("SENT_MAILBOX", "Custom")
    imap = DummyImap([], selectable={"Custom"})
    sent = detect_sent_folder(imap)
    assert sent == "Custom"
    assert cache.read_text(encoding="utf-8").strip() == "Custom"
