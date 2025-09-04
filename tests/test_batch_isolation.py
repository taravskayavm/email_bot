import asyncio

import pytest

import emailbot.bot_handlers as bh
from tests.test_bot_handlers import DummyUpdate, DummyContext


@pytest.mark.asyncio
async def test_batch_isolation(monkeypatch):
    ctx = DummyContext()
    update1 = DummyUpdate(text="http://one.example", chat_id=1)
    update2 = DummyUpdate(text="http://two.example", chat_id=1)

    async def slow_extract(url, session, chat_id, batch_id):
        await asyncio.sleep(0.2)
        if "one" in url:
            return url, {"first@example.com"}, set(), [], {}
        return url, {"second@example.com"}, set(), [], {}

    monkeypatch.setattr(bh, "async_extract_emails_from_url", slow_extract)

    task1 = asyncio.create_task(bh.handle_text(update1, ctx))
    await asyncio.sleep(0.05)
    clear_update = DummyUpdate(text="/clear", chat_id=1)
    await bh.reset_email_list(clear_update, ctx)
    await bh.handle_text(update2, ctx)
    await task1

    state = ctx.chat_data[bh.SESSION_KEY]
    assert state.to_send == ["second@example.com"]


@pytest.mark.asyncio
async def test_single_flight(monkeypatch):
    ctx = DummyContext()
    update = DummyUpdate(text="http://one.example", chat_id=1)

    counter = {"calls": 0}

    async def slow_extract(url, session, chat_id, batch_id):
        counter["calls"] += 1
        await asyncio.sleep(0.1)
        return url, {"first@example.com"}, set(), [], {}

    monkeypatch.setattr(bh, "async_extract_emails_from_url", slow_extract)

    t1 = asyncio.create_task(bh.handle_text(update, ctx))
    await asyncio.sleep(0.01)
    t2 = asyncio.create_task(bh.handle_text(update, ctx))
    await asyncio.gather(t1, t2)

    assert counter["calls"] == 1
