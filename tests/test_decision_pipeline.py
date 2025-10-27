from datetime import datetime, timezone
import logging

from emailbot.policy import decide, Decision


def test_decide_blocked_runs_before_cooldown(monkeypatch):
    called = {"can_send": False}

    monkeypatch.setattr("emailbot.policy.is_valid_email", lambda value: True)
    monkeypatch.setattr("emailbot.policy.is_role_like", lambda value: False)
    monkeypatch.setattr("emailbot.policy.is_blocked", lambda value: True)

    def _can_send(email: str, campaign: str, now: datetime) -> bool:
        called["can_send"] = True
        return True

    monkeypatch.setattr("emailbot.policy.ledger.can_send", _can_send)

    decision, reason = decide(
        "blocked@example.com",
        "cmp",
        datetime(2025, 1, 1, tzinfo=timezone.utc),
    )

    assert decision is Decision.SKIP_BLOCKED
    assert reason == "blocked"
    assert called["can_send"] is False


def test_decide_cooldown_when_not_blocked(monkeypatch):
    monkeypatch.setattr("emailbot.policy.is_valid_email", lambda value: True)
    monkeypatch.setattr("emailbot.policy.is_role_like", lambda value: False)
    monkeypatch.setattr("emailbot.policy.is_blocked", lambda value: False)
    monkeypatch.setattr("emailbot.policy.violates_domain_policy", lambda value: False)

    calls: list[tuple[str, str]] = []

    def _can_send(email: str, campaign: str, now: datetime) -> bool:
        calls.append((email, campaign))
        return False

    monkeypatch.setattr("emailbot.policy.ledger.can_send", _can_send)

    decision, reason = decide(
        "fresh@example.com",
        "campaign-a",
        datetime(2025, 1, 1, tzinfo=timezone.utc),
    )

    assert decision is Decision.SKIP_COOLDOWN
    assert reason == "cooldown"
    assert calls == [("fresh@example.com", "campaign-a")]


def test_send_email_skips_blocked_without_ledger(monkeypatch, tmp_path, caplog):
    from emailbot import messaging

    html_path = tmp_path / "tpl.html"
    html_path.write_text("<html></html>", encoding="utf-8")

    caplog.set_level(logging.INFO)

    monkeypatch.setattr(
        messaging,
        "decide",
        lambda email, campaign, now: (Decision.SKIP_BLOCKED, "blocked"),
    )

    def _record_send(*args, **kwargs):  # pragma: no cover - ensures no call
        raise AssertionError("record_send should not be called for blocked emails")

    monkeypatch.setattr(messaging.ledger, "record_send", _record_send)

    def _fail_build(*args, **kwargs):  # pragma: no cover - we must not build
        raise AssertionError("build_message should not be called when skipped")

    monkeypatch.setattr(messaging, "build_message", _fail_build)

    outcome = messaging.send_email("blocked@example.com", str(html_path))

    assert outcome is messaging.SendOutcome.BLOCKED
    assert "reason=blocked" in caplog.text

