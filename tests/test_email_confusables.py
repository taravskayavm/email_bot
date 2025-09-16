import importlib

import pytest

import config
import utils.email_clean as email_clean


@pytest.fixture(autouse=True)
def reset_flags(monkeypatch):
    monkeypatch.setattr(config, "CONFUSABLES_NORMALIZE", True, raising=False)
    monkeypatch.setattr(email_clean, "CONFUSABLES_NORMALIZE", True, raising=False)


def test_confusable_domain_label(monkeypatch):
    emails, meta = email_clean.parse_emails_unified(
        "ivan.petrov@maiл.ru", return_meta=True
    )
    assert emails == ["ivan.petrov@mail.ru"]
    assert meta["items"][0]["reason"] == "confusables-normalized"


def test_confusable_local_letters(monkeypatch):
    emails, meta = email_clean.parse_emails_unified(
        "mariа-smith@yаndex.ru", return_meta=True
    )
    assert emails == ["maria-smith@yandex.ru"]
    assert meta["items"][0]["reason"] == "confusables-normalized"


def test_punycode_domain_not_changed(monkeypatch):
    local, domain, changed = email_clean.normalize_confusables("sergey", "xn--80asehdb")
    assert (local, domain, changed) == ("sergey", "xn--80asehdb", False)


def test_confusable_fallback_on_idna_failure(monkeypatch):
    original = email_clean.normalize_domain

    def fake_normalize(domain: str):
        if domain == "mail.ru":
            return "", "invalid-idna"
        return original(domain)

    monkeypatch.setattr(email_clean, "normalize_domain", fake_normalize)

    emails, meta = email_clean.parse_emails_unified(
        "ivan.petrov@maiл.ru", return_meta=True
    )
    assert emails == ["ivan.petrov@xn--mai-ied.ru"]
    assert meta["items"][0]["reverted"] is True
    assert meta["items"][0]["reason"] is None
