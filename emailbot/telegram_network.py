"""Network workarounds for reaching the Telegram Bot API."""

from __future__ import annotations

import ipaddress
import socket
import threading
from typing import Any

TELEGRAM_API_HOST = "api.telegram.org"

_ORIGINAL_GETADDRINFO = socket.getaddrinfo
_LOCK = threading.Lock()
_override_ip: str | None = None
_installed = False


def _normalized_host(host: Any) -> str:
    if isinstance(host, bytes):
        try:
            host = host.decode("ascii")
        except UnicodeDecodeError:
            return ""
    return str(host).rstrip(".").lower()


def _telegram_getaddrinfo(
    host: Any,
    port: Any,
    family: int = 0,
    type: int = 0,
    proto: int = 0,
    flags: int = 0,
):
    forced_ip = _override_ip
    if forced_ip and _normalized_host(host) == TELEGRAM_API_HOST:
        return _ORIGINAL_GETADDRINFO(
            forced_ip,
            port,
            family,
            type,
            proto,
            flags,
        )
    return _ORIGINAL_GETADDRINFO(host, port, family, type, proto, flags)


def configure_telegram_api_ip(value: str | None) -> str | None:
    """Force only ``api.telegram.org`` to a configured IP address.

    The request URL and TLS server name remain ``api.telegram.org``. Only the
    DNS result used by this Python process is replaced.
    """

    raw = (value or "").strip()
    if not raw:
        return None

    forced_ip = str(ipaddress.ip_address(raw))

    global _installed, _override_ip
    with _LOCK:
        _override_ip = forced_ip
        if not _installed:
            socket.getaddrinfo = _telegram_getaddrinfo
            _installed = True

    return forced_ip
