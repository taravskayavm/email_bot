from datetime import datetime, timezone

import pytest

from emailbot import history_service, messaging


@pytest.fixture(autouse=True)
def _prepare_messaging(monkeypatch, tmp_path):
    monkeypatch.setattr(messaging, "LOG_FILE", str(tmp_path / "log.csv"))
    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(tmp_path / "blocked.txt"))
    monkeypatch.setattr(messaging, "is_foreign", lambda _: False)
    monkeypatch.setattr(messaging, "is_suppressed", lambda _: False)


def test_prepare_mass_mailing_respects_history():
    history_service.ensure_initialized()
    emails = ["user@example.com"]

    ready, blocked_foreign, blocked_invalid, skipped_recent, _ = (
        messaging.prepare_mass_mailing(emails, group="grp")
    )
    assert ready == ["user@example.com"]
    assert blocked_foreign == []
    assert blocked_invalid == []
    assert skipped_recent == []

    history_service.mark_sent(
        "user@example.com",
        "grp",
        "msg-1",
        datetime.now(timezone.utc),
    )

    ready2, _, _, skipped_recent2, _ = messaging.prepare_mass_mailing(
        emails, group="grp"
    )
    assert ready2 == []
    assert skipped_recent2 == ["user@example.com"]
