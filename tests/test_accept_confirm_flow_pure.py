"""Unit test for suspect auto-fix helpers used in the confirm flow."""

from __future__ import annotations

import pytest

from utils.email_clean import parse_emails_unified

try:  # pragma: no cover - import guard for optional handlers module
    from emailbot.bot_handlers import dedupe_keep_original, drop_leading_char_twins

    HAVE_BOT_HELPERS = True
except Exception:  # pragma: no cover - environments without bot handlers
    HAVE_BOT_HELPERS = False


@pytest.mark.skipif(not HAVE_BOT_HELPERS, reason="bot_helpers not importable in this env")
def test_accept_suspects_autofix_and_dedupe() -> None:
    suspects = [
        "ivanov@mail.ru",
        "aivanov@mail.ru",
        "russiaanalexan@mail.ru",
        " user.name+tag@mail.ru ",
        "USER.NAME@mail.ru",
    ]

    text_blob = "\n".join(suspects)
    fixed, meta = parse_emails_unified(text_blob, return_meta=True)
    fixed = dedupe_keep_original(fixed)
    fixed = drop_leading_char_twins(fixed)

    assert "aivanov@mail.ru" in fixed
    assert "ivanov@mail.ru" not in fixed
    suspects_meta = set(meta.get("suspects") or [])
    assert "russiaanalexan@mail.ru" in suspects_meta
    assert "russiaanalexan@mail.ru" not in fixed
    canon_like = [email for email in fixed if email.endswith("@mail.ru") and email.startswith("user.name")]
    assert len(canon_like) == 1, f"expected single canon of user.name@, got: {canon_like}"
