from __future__ import annotations

import asyncio
import logging
import types

from telegram.error import TimedOut

from emailbot import bot_handlers
from emailbot.messaging_utils import SecretFilter


def test_secret_filter_masks_telegram_token_without_explicit_secret():
    token = "1234567890:abcdefghijklmnopqrstuvwxyzABCDE1234"
    record = logging.LogRecord(
        "httpx",
        logging.INFO,
        __file__,
        1,
        f"POST https://api.telegram.org/bot{token}/getUpdates",
        (),
        None,
    )

    SecretFilter([]).filter(record)

    assert token not in record.getMessage()
    assert "/bot***/getUpdates" in record.getMessage()


def test_download_file_retries_transient_telegram_timeout(monkeypatch, tmp_path):
    calls = {"get_file": 0, "download": 0}

    class File:
        async def download_to_drive(self, path, **kwargs):
            calls["download"] += 1
            assert kwargs["read_timeout"] == 180.0

    class Document:
        file_name = "archive.zip"

        async def get_file(self, **kwargs):
            calls["get_file"] += 1
            if calls["get_file"] == 1:
                raise TimedOut("temporary")
            return File()

    async def no_delay(_seconds):
        return None

    monkeypatch.setattr(asyncio, "sleep", no_delay)
    update = types.SimpleNamespace(
        message=types.SimpleNamespace(document=Document()),
        effective_chat=types.SimpleNamespace(id=123),
    )

    result = asyncio.run(bot_handlers._download_file(update, str(tmp_path)))

    assert result.endswith("archive.zip")
    assert calls == {"get_file": 2, "download": 1}
