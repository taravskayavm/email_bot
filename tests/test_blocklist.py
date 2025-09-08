import os
from email.message import EmailMessage

from emailbot.messaging import _is_blocklisted


def _msg(to_):
    m = EmailMessage()
    m["From"] = "bot@example.com"
    m["To"] = to_
    m["Subject"] = "x"
    return m


def test_default_blocklist():
    assert _is_blocklisted("no-reply@site.com")
    assert _is_blocklisted("mailer-daemon@site.com")
    assert not _is_blocklisted("user@site.com")


def test_support_toggle(monkeypatch):
    monkeypatch.setenv("FILTER_SUPPORT", "1")
    assert _is_blocklisted("support@vendor.io")
    monkeypatch.setenv("FILTER_SUPPORT", "0")
    assert not _is_blocklisted("support@vendor.io")


def test_extra_blocklist(monkeypatch):
    monkeypatch.setenv("FILTER_BLOCKLIST", "helpdesk@x.com, alerts@news.io")
    assert _is_blocklisted("alerts@news.io")
    assert not _is_blocklisted("user@news.io")

