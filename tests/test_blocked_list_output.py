import asyncio
import types
from unittest.mock import AsyncMock

import pytest
from telegram.error import BadRequest, TimedOut

from emailbot import bot_handlers as bh
import email_bot


@pytest.mark.parametrize('count', [0, 2, 600])
def test_blocked_list_preserves_all_addresses(monkeypatch, count):
    addresses = {f'user{i:04d}@example.org' for i in range(count)}
    monkeypatch.setattr(bh, 'dedupe_blocked_file', lambda: None)
    monkeypatch.setattr(bh, 'get_blocked_emails', lambda: addresses)
    message = types.SimpleNamespace(reply_text=AsyncMock())
    asyncio.run(bh.show_blocked_list(types.SimpleNamespace(message=message), None))
    texts = [c.args[0] for c in message.reply_text.call_args_list]
    assert all(len(t.encode('utf-16-le')) // 2 <= 4096 for t in texts)
    if count:
        assert ''.join(texts).splitlines()[1:] == sorted(addresses)
    else:
        assert len(texts) == 1


def test_list_chunks_preserve_long_unicode_lines():
    message = types.SimpleNamespace(reply_text=AsyncMock())
    original = '\U0001f600' * 5000 + '\n' + 'x' * 4500
    asyncio.run(bh._reply_list_text(message, original))
    calls = message.reply_text.call_args_list
    assert ''.join(c.args[0] for c in calls) == original
    assert all(len(c.args[0].encode('utf-16-le')) // 2 <= 4096 for c in calls)
    assert all(c.kwargs['parse_mode'] is None for c in calls)


def test_blocked_domains_split(monkeypatch):
    domains = {f'domain{i:04d}.org' for i in range(600)}
    monkeypatch.setattr(bh.blocked_domains, 'load_blocked_domains', lambda: domains)
    message = types.SimpleNamespace(reply_text=AsyncMock())
    asyncio.run(bh.show_blocked_domains(types.SimpleNamespace(message=message), None))
    texts = [c.args[0] for c in message.reply_text.call_args_list]
    assert len(texts) > 1
    assert ''.join(texts).splitlines()[1:] == sorted(domains)
    assert all(len(t.encode('utf-16-le')) // 2 <= 4096 for t in texts)


@pytest.mark.parametrize('error,network', [(BadRequest('Message is too long'), False), (TimedOut(), True)])
def test_error_message_distinguishes_bad_request(error, network):
    bot = types.SimpleNamespace(send_message=AsyncMock())
    context = types.SimpleNamespace(error=error, bot=bot)
    update = types.SimpleNamespace(effective_chat=types.SimpleNamespace(id=123))
    asyncio.run(email_bot.error_handler(update, context))
    text = bot.send_message.call_args.kwargs['text']
    assert ('Telegram' in text) == network
