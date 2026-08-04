"""Smoke tests for ``utils.email_role`` heuristics."""

from utils.email_role import classify_email_role


def test_role_local_basic() -> None:
    result = classify_email_role("no-reply", "example.com", "")
    assert result["class"] == "role"
    assert result["reason"] == "role-local"


def test_personal_hint_in_context() -> None:
    result = classify_email_role("ivan.ivanov", "university.ru", "Corresponding author")
    assert result["class"] in {"personal", "unknown"}
    assert result["score"] >= 0.5


def test_unknown_default() -> None:
    result = classify_email_role("abc", "example.com", "random text")
    assert result["class"] in {"unknown", "personal", "role"}
    assert 0.0 <= result["score"] <= 1.0


def test_public_mail_domain_does_not_make_personal_addresses_role_accounts() -> None:
    for local in ("arishok25", "drmark1982", "veronika1306"):
        result = classify_email_role(local, "mail.ru", "")
        assert result["class"] != "role", (local, result)


def test_explicit_editorial_local_parts_remain_role_accounts() -> None:
    for local in ("cardio.nauka", "cardiovasc.journal"):
        result = classify_email_role(local, "yandex.ru", "")
        assert result["class"] == "role", (local, result)
