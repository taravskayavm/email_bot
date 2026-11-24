import asyncio
import socket

import httpx
import pytest
from telegram.error import NetworkError
from telegram.request import HTTPXRequest

from emailbot import telegram_network as net


def test_parse_addresses():
    assert net.parse_telegram_api_ips(" 149.154.167.220,149.154.166.110,149.154.167.220 ") == (
        "149.154.167.220", "149.154.166.110"
    )
    assert net.parse_telegram_api_ips("") == ()
    with pytest.raises(ValueError):
        net.parse_telegram_api_ips("149.154.167.220,invalid")


@pytest.mark.parametrize("error", [httpx.ConnectError, httpx.ConnectTimeout])
def test_failover_remembers_success_and_restores_context(monkeypatch, error):
    calls = []
    async def fake(self, *args, **kwargs):
        route = net._request_route.get()
        calls.append(route)
        if route == "149.154.167.220":
            raise NetworkError("connect failed") from error("TLS or connect failure")
        return 200, b"ok"
    monkeypatch.setattr(HTTPXRequest, "do_request", fake)
    async def run():
        request = net.FailoverHTTPXRequest(("149.154.167.220", "149.154.166.110"))
        try:
            assert await request.do_request("https://api.telegram.org/test", "POST") == (200, b"ok")
            assert net._request_route.get() == ""
            await request.do_request("https://api.telegram.org/test", "POST")
        finally:
            await request.shutdown()
    asyncio.run(run())
    assert calls == ["149.154.167.220", "149.154.166.110", "149.154.166.110"]


@pytest.mark.parametrize("error", [httpx.ReadTimeout, httpx.WriteError, httpx.PoolTimeout])
def test_does_not_replay_ambiguous_or_pool_failures(monkeypatch, error):
    calls = []
    async def fake(self, *args, **kwargs):
        calls.append(net._request_route.get())
        raise NetworkError("failed") from error("failed")
    monkeypatch.setattr(HTTPXRequest, "do_request", fake)
    async def run():
        request = net.FailoverHTTPXRequest(("149.154.167.220",))
        try:
            with pytest.raises(NetworkError):
                await request.do_request("https://api.telegram.org/test", "POST")
            assert net._request_route.get() == ""
        finally:
            await request.shutdown()
    asyncio.run(run())
    assert calls == ["149.154.167.220"]


def test_exhaustion_tries_dns_once(monkeypatch):
    calls = []
    async def fake(self, *args, **kwargs):
        calls.append(net._request_route.get())
        raise NetworkError("failed") from httpx.ConnectError("failed")
    monkeypatch.setattr(HTTPXRequest, "do_request", fake)
    async def run():
        request = net.FailoverHTTPXRequest(("149.154.167.220",))
        try:
            with pytest.raises(NetworkError):
                await request.do_request("https://api.telegram.org/test", "POST")
            assert net._request_route.get() == ""
        finally:
            await request.shutdown()
    asyncio.run(run())
    assert calls == ["149.154.167.220", None]


def test_route_controls_dns_and_preserves_other_hosts(monkeypatch):
    monkeypatch.setattr(net, "_override_ip", "149.154.166.110")
    monkeypatch.setattr(net, "_ORIGINAL_GETADDRINFO", lambda host, *args: [host])
    token = net._request_route.set("149.154.167.220")
    try:
        assert net._telegram_getaddrinfo(b"api.telegram.org", 443) == ["149.154.167.220"]
        assert net._telegram_getaddrinfo("example.org", 443) == ["example.org"]
        net._request_route.set(None)
        assert net._telegram_getaddrinfo("api.telegram.org", 443) == ["api.telegram.org"]
    finally:
        net._request_route.reset(token)
