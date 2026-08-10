"""Compatibility tests for URL buttons sent before deep crawl was removed."""

import asyncio
import types

import pytest

from conftest import MISSING_OPTIONALS

if "aiogram" in MISSING_OPTIONALS:
    pytest.skip("aiogram is not installed", allow_module_level=True)

from emailbot.bot.handlers import ingest


@pytest.mark.optional
def test_old_deep_callback_is_downgraded_to_single_page(monkeypatch):
    calls: list[dict[str, object]] = []

    async def fake_process(callback, *, deep, limit_pages=None):
        calls.append(
            {"callback": callback, "deep": deep, "limit_pages": limit_pages}
        )

    monkeypatch.setattr(ingest, "_process_url_callback", fake_process)
    callback = types.SimpleNamespace()

    asyncio.run(ingest.parse_deep(callback))

    assert calls == [
        {"callback": callback, "deep": False, "limit_pages": None}
    ]
