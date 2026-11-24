"""Batch isolation through the current URL handler, with no live HTTP."""
import asyncio
from unittest.mock import AsyncMock

import pytest
from telegram.ext import ApplicationHandlerStop

from emailbot import bot_handlers as bh, digest
from tests.test_bot_handlers import DummyUpdate, DummyContext


@pytest.fixture
def url_context(monkeypatch):
    monkeypatch.setattr(bh.settings, "ENABLE_WEB", True)
    monkeypatch.setattr(bh, "_compose_report_and_save", AsyncMock(return_value="done"))
    monkeypatch.setattr(bh, "_send_combined_parse_response", AsyncMock())
    return DummyContext()


async def parse(update, context):
    try:
        await bh.handle_url_text(update, context)
    except ApplicationHandlerStop:
        pass


@pytest.mark.asyncio
async def test_batch_isolation(monkeypatch, url_context):
    started, release = asyncio.Event(), asyncio.Event()

    async def extract(url, **kwargs):
        if "one" in url:
            started.set()
            await release.wait()
            return ["first@example.com"]
        return ["second@example.com"]

    monkeypatch.setattr(digest, "extract_from_url", extract)
    first = asyncio.create_task(parse(DummyUpdate(text="https://one.example", chat_id=1), url_context))
    await started.wait()
    await bh.reset_email_list(DummyUpdate(text="/clear", chat_id=1), url_context)
    await parse(DummyUpdate(text="https://two.example", chat_id=1), url_context)
    release.set()
    await asyncio.gather(first, return_exceptions=True)
    assert bh.get_state(url_context).to_send == ["second@example.com"]


@pytest.mark.asyncio
async def test_single_flight(monkeypatch, url_context):
    started, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def extract(url, **kwargs):
        calls.append(url)
        started.set()
        await release.wait()
        return ["first@example.com"]

    monkeypatch.setattr(digest, "extract_from_url", extract)
    update = DummyUpdate(text="https://one.example", chat_id=1)
    first = asyncio.create_task(parse(update, url_context))
    await started.wait()
    await parse(update, url_context)
    release.set()
    await first
    assert len(calls) == 1
