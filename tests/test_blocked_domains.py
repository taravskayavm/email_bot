from __future__ import annotations

from pathlib import Path

import pytest

from emailbot import blocked_domains, messaging
from emailbot.handlers.preview import _collect_blocked


@pytest.fixture
def domain_store(tmp_path: Path):
    original = blocked_domains.blocked_domains_path()
    target = tmp_path / "blocked_domains.txt"
    blocked_domains.init_blocked_domains(target)
    try:
        yield target
    finally:
        blocked_domains.init_blocked_domains(original)


def test_domain_store_starts_with_requested_defaults(domain_store: Path) -> None:
    assert blocked_domains.load_blocked_domains() == {
        "163.com",
        "cardiotomsk.ru",
        "emcmos.ru",
        "ion.com",
        "ngs.ru",
        "npcmr.ru",
        "qq.com",
    }
    assert domain_store.read_text(encoding="utf-8").splitlines() == sorted(
        blocked_domains.DEFAULT_BLOCKED_DOMAINS
    )


def test_domain_store_accepts_menu_friendly_inputs(domain_store: Path) -> None:
    valid, rejected = blocked_domains.parse_domains(
        "Example.org, @MAIL.EXAMPLE.NET https://dept.example.edu/path invalid_domain"
    )

    assert valid == ["example.org", "mail.example.net", "dept.example.edu"]
    assert rejected == ["invalid_domain"]
    assert blocked_domains.add_blocked_domains(valid) == 3
    assert blocked_domains.is_blocked_domain("sub.example.org") is True
    assert blocked_domains.add_blocked_domain("EXAMPLE.ORG") is False


def test_prepare_mass_mailing_excludes_blocked_domains_but_keeps_gmail(
    domain_store: Path, monkeypatch
) -> None:
    monkeypatch.setattr(messaging, "get_blocked_emails", lambda: set())
    monkeypatch.setattr(messaging.rules, "is_blocked", lambda email: False)
    monkeypatch.setattr(messaging, "is_suppressed", lambda email: False)

    ready, blocked_foreign, blocked_invalid, skipped_recent, digest = (
        messaging.prepare_mass_mailing(
            [
                "author@gmail.com",
                "editor@cardiotomsk.ru",
                "office@sub.qq.com",
            ],
            group="medicine",
            ignore_cooldown=True,
        )
    )

    assert ready == ["author@gmail.com"]
    assert blocked_foreign == []
    assert blocked_invalid == ["editor@cardiotomsk.ru", "office@sub.qq.com"]
    assert skipped_recent == []
    assert digest["blocked_domain"] == 2
    assert digest["removed_blocked_domain"] == 2
    assert messaging._is_blocklisted("person@163.com") is True


def test_send_guard_blocks_domain_before_smtp(monkeypatch) -> None:
    monkeypatch.setattr(messaging, "_is_blocklisted", lambda email: True)

    outcome, raw, code, error = messaging.send_email_with_sessions(
        client=object(),
        imap=None,
        sent_folder="Sent",
        recipient="person@qq.com",
        html_path="unused.html",
    )

    assert outcome is messaging.SendOutcome.BLOCKED
    assert (raw, code, error) == ("", None, None)


def test_preview_marks_domain_exclusion_separately(domain_store: Path) -> None:
    blocked_rows, foreign_rows = _collect_blocked(
        [], ["editor@cardiotomsk.ru", "person@example.ru"]
    )

    assert foreign_rows == []
    reasons = {row["email"]: row["reason"] for row in blocked_rows}
    assert reasons == {
        "editor@cardiotomsk.ru": "blocked_domain",
        "person@example.ru": "blocked",
    }
