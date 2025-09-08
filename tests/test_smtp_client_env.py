import smtplib

import pytest

from emailbot.smtp_client import SmtpClient


class DummySMTP:
    def __init__(self):
        self.starttls_called = False
        self.login_called = False

    def starttls(self, context=None):
        self.starttls_called = True

    def login(self, user, password):
        self.login_called = True

    def quit(self):
        pass

    def sendmail(self, *a, **k):
        pass


def test_smtp_client_uses_ssl_from_env(monkeypatch):
    dummy = DummySMTP()
    ssl_called = False

    def fake_ssl(*args, **kwargs):
        nonlocal ssl_called
        ssl_called = True
        return dummy

    monkeypatch.setattr(smtplib, "SMTP_SSL", fake_ssl)
    monkeypatch.setattr(smtplib, "SMTP", lambda *a, **k: dummy)
    monkeypatch.setenv("SMTP_SSL", "1")

    with SmtpClient("host", 587, "user", "pass"):
        pass

    assert ssl_called
    assert not dummy.starttls_called


def test_smtp_client_uses_starttls_when_ssl_disabled(monkeypatch):
    dummy = DummySMTP()
    smtp_called = False

    def fake_smtp(*args, **kwargs):
        nonlocal smtp_called
        smtp_called = True
        return dummy

    monkeypatch.setattr(smtplib, "SMTP", fake_smtp)
    monkeypatch.setattr(smtplib, "SMTP_SSL", lambda *a, **k: dummy)
    monkeypatch.setenv("SMTP_SSL", "0")

    with SmtpClient("host", 587, "user", "pass"):
        pass

    assert smtp_called
    assert dummy.starttls_called
