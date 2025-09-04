import pytest

import emailbot.bot_handlers as bh
import emailbot.messaging as messaging
from tests.test_bot_handlers import DummyUpdate, DummyContext


@pytest.mark.asyncio
async def test_preview_after_filters(monkeypatch):
    ctx = DummyContext()
    ctx.chat_data[bh.SESSION_KEY] = bh.SessionState(to_send=["user@example.com"])

    def fake_prepare(emails):
        assert emails == ["user@example.com"]
        return [], [], [], [], {
            "input_total": 1,
            "after_suppress": 0,
            "foreign_blocked": 0,
            "after_180d": 0,
            "sent_planned": 0,
            "skipped_by_dup_in_batch": 0,
            "unique_ready_to_send": 0,
            "skipped_suppress": 1,
            "skipped_180d": 0,
            "skipped_foreign": 0,
        }

    monkeypatch.setattr(messaging, "prepare_mass_mailing", fake_prepare)

    update = DummyUpdate(callback_data="group_спорт", chat_id=1)
    await bh.select_group(update, ctx)

    assert "блок-листах" in update.callback_query.message.replies[0]
    assert update.callback_query.message.reply_markups[0] is None
