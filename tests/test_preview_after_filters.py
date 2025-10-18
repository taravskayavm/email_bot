import pytest

pytest.importorskip("emailbot.bot_handlers")

import emailbot.bot_handlers as bh
import emailbot.messaging as messaging
from tests.test_bot_handlers import DummyUpdate, DummyContext


@pytest.mark.asyncio
async def test_preview_after_filters(monkeypatch, tmp_path):
    ctx = DummyContext()
    ctx.chat_data[bh.SESSION_KEY] = bh.SessionState(to_send=["user@example.com"])

    def fake_prepare(emails, group, chat_id=None, **kwargs):
        assert emails == ["user@example.com"]
        assert group == "sport"
        return [], [], [], [], {
            "input_total": 1,
            "after_suppress": 0,
            "foreign_blocked": 0,
            "ready_after_cooldown": 0,
            "sent_planned": 0,
            "removed_duplicates_in_batch": 0,
            "unique_ready_to_send": 0,
            "skipped_suppress": 1,
            "skipped_180d": 0,
            "skipped_foreign": 0,
        }

    monkeypatch.setattr(messaging, "prepare_mass_mailing", fake_prepare)

    tpl_path = tmp_path / "sport.html"
    tpl_path.write_text("<html></html>", encoding="utf-8")
    monkeypatch.setattr(
        bh,
        "get_template",
        lambda code: {
            "code": code,
            "label": code.title(),
            "path": str(tpl_path),
        }
        if code == "sport"
        else None,
    )

    update = DummyUpdate(callback_data="tpl:sport", chat_id=1)
    await bh.select_group(update, ctx)

    assert any("стоп-листах" in text for text in update.callback_query.message.replies)
    assert update.callback_query.message.reply_markups[-1] is None
