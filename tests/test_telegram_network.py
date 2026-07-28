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
    socket.getaddrinfo("api.telegram.org", 443, socket.AF_INET)
    socket.getaddrinfo("example.com", 443, socket.AF_INET)
    assert calls[0][0] == "149.154.167.220"
    assert calls[1][0] == "example.com"


def test_configure_telegram_api_ip_rejects_invalid_address():
    with pytest.raises(ValueError):
        telegram_network.configure_telegram_api_ip("not-an-ip")
