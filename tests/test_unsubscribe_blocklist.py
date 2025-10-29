from pathlib import Path

from emailbot import messaging


def test_mark_unsubscribed_writes_to_blocklist(tmp_path, monkeypatch, request):
    target = tmp_path / "blocked_emails.txt"

    original_init = messaging.suppress_list.init_blocked
    original_path = messaging.suppress_list.blocklist_path()
    original_block_ready = getattr(messaging, "_BLOCK_READY", False)

    def restore_original_path() -> None:
        original_init(path=original_path)
        messaging.suppress_list.invalidate_cache()
        messaging._BLOCK_READY = original_block_ready

    request.addfinalizer(restore_original_path)

    def init_blocked(path: str | Path | None = None) -> None:
        original_init(path=str(target))

    monkeypatch.setattr(messaging.suppress_list, "init_blocked", init_blocked)
    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(target), raising=False)
    monkeypatch.setattr(messaging, "_BLOCK_READY", False, raising=False)
    messaging.suppress_list.invalidate_cache()
    messaging.suppress_list.init_blocked(path=str(target))

    addr = "User.Example+tag@GMAIL.com"
    messaging.mark_unsubscribed(addr, token="t")

    assert target.exists()
    content = target.read_text(encoding="utf-8").splitlines()
    expected = messaging.normalize_email(addr)
    assert expected in content

    messaging.mark_unsubscribed(addr, token="t")
    content2 = target.read_text(encoding="utf-8").splitlines()
    assert content2.count(expected) == 1
