import pytest


def _prepare_blocklist(monkeypatch, messaging_mod, rules_mod, suppress_list_mod, path):
    monkeypatch.setattr(messaging_mod, "BLOCKED_FILE", str(path))
    monkeypatch.setattr(rules_mod, "BLOCKLIST_PATH", path)
    suppress_list_mod.init_blocked(str(path))


def test_role_like_filter_allows_digit_prefixed(monkeypatch, tmp_path):
    """Digit-prefixed locals should not be filtered out by the mass-mailing pipeline."""

    var = tmp_path / "var"
    var.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SENT_LOG_PATH", str(var / "sent_log.csv"))
    monkeypatch.setenv("SYNC_STATE_PATH", str(var / "sync_state.json"))

    blocked = tmp_path / "blocked_emails.txt"
    blocked.write_text("", encoding="utf-8")
    monkeypatch.setenv("BLOCKED_LIST_PATH", str(blocked))
    monkeypatch.setenv("BLOCKED_EMAILS_PATH", str(blocked))

    from emailbot import messaging, suppress_list
    from utils import rules

    _prepare_blocklist(monkeypatch, messaging, rules, suppress_list, blocked)

    emails = ["123ivanov@college.ru", "ok@domain.com"]
    ready, blocked_foreign, blocked_invalid, skipped_recent, digest = messaging.prepare_mass_mailing(
        emails, group="grp", chat_id=None, ignore_cooldown=True
    )

    ready_lower = {e.lower() for e in ready}
    assert ready_lower == {"123ivanov@college.ru", "ok@domain.com"}
    assert not blocked_foreign
    assert not blocked_invalid
    assert not skipped_recent
    assert digest.get("skipped_suppress", 0) == 0


@pytest.mark.xfail(reason="Manual approval bypass for role-like addresses not implemented yet")
def test_role_like_manual_approval_future(monkeypatch, tmp_path):
    """Placeholder ensuring we notice behaviour changes around strict role filters."""

    var = tmp_path / "var"
    var.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SENT_LOG_PATH", str(var / "sent_log.csv"))
    monkeypatch.setenv("SYNC_STATE_PATH", str(var / "sync_state.json"))

    blocked = tmp_path / "blocked_emails.txt"
    blocked.write_text("", encoding="utf-8")
    monkeypatch.setenv("BLOCKED_LIST_PATH", str(blocked))
    monkeypatch.setenv("BLOCKED_EMAILS_PATH", str(blocked))

    from emailbot import messaging, suppress_list
    from utils import rules

    _prepare_blocklist(monkeypatch, messaging, rules, suppress_list, blocked)

    ready, *_ = messaging.prepare_mass_mailing(["support@support.com"], ignore_cooldown=True)
    # Once manual approval learns to bypass role-like classification we expect this to stay.
    assert "support@support.com" not in {e.lower() for e in ready}
