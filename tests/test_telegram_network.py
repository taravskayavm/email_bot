from __future__ import annotations

import socket

import pytest

from emailbot import telegram_network


def test_configure_telegram_api_ip_overrides_only_telegram(monkeypatch):
    calls = []

    def fake_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        calls.append((host, port, family, type, proto, flags))
        return [(family, type, proto, "", (str(host), port))]

    monkeypatch.setattr(telegram_network, "_ORIGINAL_GETADDRINFO", fake_getaddrinfo)
    monkeypatch.setattr(telegram_network, "_override_ip", None)
    monkeypatch.setattr(telegram_network, "_installed", False)
    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    configured = telegram_network.configure_telegram_api_ip("149.154.167.220")

    assert configured == "149.154.167.220"
    results = socket.getaddrinfo("api.telegram.org", 443, socket.AF_INET)
    socket.getaddrinfo("example.com", 443, socket.AF_INET)
    assert calls[0][0] == "149.154.167.220"
    assert calls[1][0] == "api.telegram.org"
    assert calls[2][0] == "example.com"
    assert [item[4][0] for item in results] == [
        "149.154.167.220",
        "api.telegram.org",
    ]


def test_configured_ip_still_works_when_dns_lookup_fails(monkeypatch):
    def fake_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        if host == "api.telegram.org":
            raise socket.gaierror("dns unavailable")
        return [(family, type, proto, "", (str(host), port))]

    monkeypatch.setattr(telegram_network, "_ORIGINAL_GETADDRINFO", fake_getaddrinfo)
    monkeypatch.setattr(telegram_network, "_override_ip", None)
    monkeypatch.setattr(telegram_network, "_installed", False)
    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    telegram_network.configure_telegram_api_ip("149.154.166.110")
    results = socket.getaddrinfo("api.telegram.org", 443, socket.AF_INET)

    assert results[0][4][0] == "149.154.166.110"


def test_configure_telegram_api_ip_rejects_invalid_address():
    with pytest.raises(ValueError):
        telegram_network.configure_telegram_api_ip("not-an-ip")
