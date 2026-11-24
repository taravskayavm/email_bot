from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from telegram.error import BadRequest, NetworkError
from telegram.request import HTTPXRequest

from emailbot import telegram_network as net


@pytest.mark.parametrize(
    'error',
    [
        httpx.ReadError,
        httpx.ReadTimeout,
        httpx.WriteError,
        httpx.WriteTimeout,
        httpx.RemoteProtocolError,
    ],
)
def test_polling_retries_transport_errors_without_changing_parameters(monkeypatch, error):
    calls = []
    data = object()

    async def fake(self, url, method, request_data=None, **kwargs):
        calls.append((net._request_route.get(), url, method, request_data, kwargs))
        if len(calls) == 1:
            raise NetworkError('temporary polling failure') from error('temporary')
        return 200, b'ok'

    monkeypatch.setattr(HTTPXRequest, 'do_request', fake)

    async def run():
        async with net.FailoverHTTPXRequest(('149.154.167.220',)) as request:
            result = await request.do_request(
                'https://api.telegram.org/test/getUpdates',
                'POST',
                request_data=data,
                read_timeout=35,
            )
            assert result == (200, b'ok')
            assert net._request_route.get() == ''

    asyncio.run(run())
    assert [call[0] for call in calls] == ['149.154.167.220', None]
    assert calls[0][1:] == calls[1][1:]


@pytest.mark.parametrize('endpoint', ['sendMessage', 'deleteWebhook'])
@pytest.mark.parametrize('error', [httpx.ReadError, httpx.RemoteProtocolError])
def test_ambiguous_non_polling_failures_are_not_replayed(monkeypatch, endpoint, error):
    calls = []

    async def fake(self, *args, **kwargs):
        calls.append(net._request_route.get())
        raise NetworkError('ambiguous failure') from error('temporary')

    monkeypatch.setattr(HTTPXRequest, 'do_request', fake)

    async def run():
        async with net.FailoverHTTPXRequest(('149.154.167.220',)) as request:
            with pytest.raises(NetworkError):
                await request.do_request(f'https://api.telegram.org/test/{endpoint}', 'POST')
            assert net._request_route.get() == ''

    asyncio.run(run())
    assert calls == ['149.154.167.220']


def test_polling_failover_does_not_reuse_previous_route_connection(monkeypatch):
    original_init = httpx.AsyncClient.__init__
    used_routes = []

    def client_init(self, **kwargs):
        connection = {}

        def respond(request):
            # Emulate a persistent connection: DNS is consulted only once.
            route = connection.setdefault('route', net._request_route.get())
            used_routes.append(route)
            if len(used_routes) == 2:
                return httpx.Response(502, json={'ok': False, 'description': 'Bad Gateway'})
            return httpx.Response(200, json={'ok': True, 'result': route})

        kwargs['transport'] = httpx.MockTransport(respond)
        kwargs['trust_env'] = False
        original_init(self, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, '__init__', client_init)

    async def run():
        request = net.FailoverHTTPXRequest(('149.154.167.220', '149.154.166.110'))
        async with request:
            url = 'https://api.telegram.org/test/getUpdates'
            assert await request.post(url) == '149.154.167.220'
            assert await request.post(url) == '149.154.166.110'
        # Every route is closed and can be initialized again.
        assert all(r._client.is_closed for r in request._route_requests)
        async with request:
            assert await request.post(url) == '149.154.166.110'
        assert all(r._client.is_closed for r in request._route_requests)
        assert net._request_route.get() == ''

    asyncio.run(run())
    assert used_routes == [
        '149.154.167.220',
        '149.154.167.220',
        '149.154.166.110',
        '149.154.166.110',
    ]


@pytest.mark.parametrize(
    'endpoint,expected_routes',
    [
        ('getUpdates', ['149.154.167.220', None]),
        ('sendMessage', ['149.154.167.220']),
    ],
)
def test_bad_gateway_exhaustion_remains_a_network_error(monkeypatch, endpoint, expected_routes):
    calls = []

    async def fake(self, *args, **kwargs):
        calls.append(net._request_route.get())
        return 502, b'{"ok":false,"description":"Bad Gateway"}'

    monkeypatch.setattr(HTTPXRequest, 'do_request', fake)

    async def run():
        async with net.FailoverHTTPXRequest(('149.154.167.220',)) as request:
            with pytest.raises(NetworkError, match='Bad Gateway'):
                await request.post(f'https://api.telegram.org/test/{endpoint}')
            assert request._preferred_route == 0
            assert net._request_route.get() == ''

    asyncio.run(run())
    assert calls == expected_routes


@pytest.mark.parametrize(
    'error,level',
    [
        (NetworkError('temporary connection failure'), logging.WARNING),
        (BadRequest('invalid request'), logging.ERROR),
        (ValueError('handler bug'), logging.ERROR),
    ],
)
def test_polling_network_error_is_not_reported_as_handler_crash(caplog, error, level):
    import email_bot

    bot = SimpleNamespace(send_message=AsyncMock())
    with caplog.at_level(logging.WARNING, logger='email_bot'):
        asyncio.run(email_bot.error_handler(None, SimpleNamespace(error=error, bot=bot)))
    records = [record for record in caplog.records if record.name == 'email_bot']
    assert len(records) == 1
    assert records[0].levelno == level
    if level == logging.WARNING:
        assert 'polling will retry' in records[0].getMessage()
        assert records[0].exc_info is None
    else:
        assert records[0].getMessage() == 'Unhandled exception'
        assert records[0].exc_info is not None
    bot.send_message.assert_not_called()
