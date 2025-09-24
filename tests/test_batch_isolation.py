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

    await bh.handle_text(update1, ctx)
    token1 = next(
        key
        for key, value in ctx.user_data.get("parse_mode_urls", {}).items()
        if "one" in value
    )
    callback1 = DummyUpdate(chat_id=1, callback_data=f"parse|deep|{token1}")
    task1 = asyncio.create_task(bh.parse_mode_cb(callback1, ctx))
    await asyncio.sleep(0.05)
    clear_update = DummyUpdate(text="/clear", chat_id=1)
    await bh.reset_email_list(clear_update, ctx)
    await bh.handle_text(update2, ctx)
    token2 = next(
        key
        for key, value in ctx.user_data.get("parse_mode_urls", {}).items()
        if "two" in value
    )
    callback2 = DummyUpdate(chat_id=1, callback_data=f"parse|deep|{token2}")
    await bh.parse_mode_cb(callback2, ctx)
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

    await bh.handle_text(update, ctx)
    token = next(iter(ctx.user_data.get("parse_mode_urls", {})))
    cb_update = DummyUpdate(chat_id=1, callback_data=f"parse|deep|{token}")
    t1 = asyncio.create_task(bh.parse_mode_cb(cb_update, ctx))
    await asyncio.sleep(0.01)
    # Повторный клик по кнопке не должен запускать второй парсер
    cb_update_again = DummyUpdate(chat_id=1, callback_data=f"parse|deep|{token}")
    await bh.parse_mode_cb(cb_update_again, ctx)
    await t1

    assert counter["calls"] == 1
