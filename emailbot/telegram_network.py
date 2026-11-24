"""Network workarounds for reaching the Telegram Bot API."""

from __future__ import annotations

import ipaddress
import logging
import socket
import ssl
import threading
import warnings
from contextvars import ContextVar
from typing import Any

import httpx
from telegram.error import NetworkError
from telegram.request import HTTPXRequest

TELEGRAM_API_HOST = "api.telegram.org"

_ORIGINAL_GETADDRINFO = socket.getaddrinfo
_LOCK = threading.Lock()
_override_ip: str | None = None
_installed = False
_request_route: ContextVar[str | None] = ContextVar('telegram_route', default='')


def parse_telegram_api_ips(value: str | None) -> tuple[str, ...]:
    """Validate and deduplicate a comma-separated list before changing state."""
    return tuple(
        dict.fromkeys(
            str(ipaddress.ip_address(part.strip()))
            for part in (value or '').split(',')
            if part.strip()
        )
    )


def _verify_telegram_hostname(certificate: dict) -> None:
    """Check the canonical API name using SANs, after normal CA validation."""
    # SSLObject cannot separate its SNI name from its automatic hostname check.
    # Validate the canonical name explicitly before HTTP can send any data.
    # Do not allow the legacy commonName fallback.
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)
        ssl.match_hostname(
            {'subjectAltName': certificate.get('subjectAltName', ())}, TELEGRAM_API_HOST
        )


class _TelegramSSLObject(ssl.SSLObject):
    def do_handshake(self) -> None:
        super().do_handshake()  # OpenSSL validates the complete certificate chain.
        _verify_telegram_hostname(self.getpeercert())


class _TelegramSNIContext(ssl.SSLContext):
    def __new__(cls):
        return super().__new__(cls, ssl.PROTOCOL_TLS_CLIENT)

    def __init__(self):
        self._normal = httpx.create_ssl_context()
        self._telegram = httpx.create_ssl_context()
        # This context is used only for api.telegram.org. The SSLObject checks
        # its canonical hostname immediately after the verified handshake.
        self._telegram.check_hostname = False
        self._telegram.sslobject_class = _TelegramSSLObject

    def wrap_bio(self, incoming, outgoing, server_side=False, server_hostname=None, session=None):
        context = self._normal
        if not server_side and server_hostname == TELEGRAM_API_HOST:
            context = self._telegram
            server_hostname += '.'
        return context.wrap_bio(
            incoming,
            outgoing,
            server_side=server_side,
            server_hostname=server_hostname,
            session=session,
        )


class _TelegramSNIRequest(HTTPXRequest):
    def _build_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(**dict(self._client_kwargs, verify=_TelegramSNIContext()))


class FailoverHTTPXRequest(HTTPXRequest):
    """Use separate pools per route and safely retry Telegram polling."""

    def __init__(self, api_ips: tuple[str, ...], **kwargs):
        super().__init__(**kwargs)
        self._routes = (*api_ips, None)  # Last route uses ordinary DNS.
        self._preferred_route = 0
        # A DNS change cannot move a connection already cached by HTTPX.
        # Separate clients ensure each retry uses the chosen route.
        self._route_requests = (self, *(HTTPXRequest(**kwargs) for _ in api_ips))
        self._request_options = kwargs
        self._sni_requests: dict[int, HTTPXRequest] = {}

    async def initialize(self) -> None:
        for request in (*self._route_requests, *self._sni_requests.values()):
            await HTTPXRequest.initialize(request)

    async def shutdown(self) -> None:
        for request in (*self._route_requests, *self._sni_requests.values()):
            await HTTPXRequest.shutdown(request)

    async def _do_route_request(self, index, url, method, *args, **kwargs):
        request = self._sni_requests.get(index, self._route_requests[index])
        try:
            return await HTTPXRequest.do_request(request, url, method, *args, **kwargs)
        except NetworkError as exc:
            # This specific error happens during TLS, before Telegram can
            # receive the HTTP request. Retry it once with a dotted SNI name.
            if (
                isinstance(request, _TelegramSNIRequest)
                or httpx.URL(url).host != TELEGRAM_API_HOST
                or not isinstance(exc.__cause__, httpx.ConnectError)
                or 'INVALID_SESSION_ID' not in str(exc.__cause__)
            ):
                raise
            fallback = self._sni_requests.get(index)
            if fallback is None:
                fallback = _TelegramSNIRequest(**self._request_options)
                self._sni_requests[index] = fallback
            logging.getLogger(__name__).warning(
                'Telegram TLS session failed via %s; retrying with verified alternate SNI',
                self._routes[index] or 'DNS',
            )
            return await HTTPXRequest.do_request(fallback, url, method, *args, **kwargs)

    async def do_request(self, url: str, method: str, *args, **kwargs):
        polling = httpx.URL(url).path.endswith('/getUpdates')
        start = self._preferred_route
        for offset in range(len(self._routes)):
            index = (start + offset) % len(self._routes)
            route = self._routes[index]
            token = _request_route.set(route)
            try:
                result = await self._do_route_request(index, url, method, *args, **kwargs)
                if polling and 500 <= result[0] < 600:
                    if offset + 1 == len(self._routes):
                        return result
                    logging.getLogger(__name__).warning(
                        'Telegram polling returned HTTP %s via %s; trying next route',
                        result[0],
                        route or 'DNS',
                    )
                    continue
                self._preferred_route = index
                return result
            except NetworkError as exc:
                # Read/write failures may occur after Telegram accepted a message.
                # Replaying those requests could send duplicates.
                safe_failure = isinstance(exc.__cause__, httpx.ConnectError | httpx.ConnectTimeout)
                if polling:
                    safe_failure = safe_failure or isinstance(
                        exc.__cause__,
                        httpx.ReadError
                        | httpx.ReadTimeout
                        | httpx.WriteError
                        | httpx.WriteTimeout
                        | httpx.RemoteProtocolError,
                    )
                if not safe_failure:
                    raise
                if offset + 1 == len(self._routes):
                    raise
                logging.getLogger(__name__).warning(
                    'Telegram connection failed via %s; trying next route', route or 'DNS'
                )
            finally:
                _request_route.reset(token)


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
    request_route = _request_route.get()
    forced_ip = _override_ip if request_route == "" else request_route
    if forced_ip and _normalized_host(host) == TELEGRAM_API_HOST:
        pinned = _ORIGINAL_GETADDRINFO(
            forced_ip,
            port,
            family,
            type,
            proto,
            flags,
        )
        if request_route != '':
            # Try TLS on this route before moving to another address. A TCP-only
            # DNS fallback cannot recover from a failed TLS handshake.
            return pinned
        # Keep the explicitly configured address first, but also return the
        # current DNS answers.  Socket clients can then fall back when the
        # pinned Telegram route becomes unreachable without requiring another
        # configuration edit and process restart.
        try:
            resolved = _ORIGINAL_GETADDRINFO(
                host,
                port,
                family,
                type,
                proto,
                flags,
            )
        except OSError:
            resolved = []

        combined = []
        seen = set()
        for item in [*pinned, *resolved]:
            try:
                key = (item[0], item[1], item[2], item[4])
            except (IndexError, TypeError):
                key = repr(item)
            if key in seen:
                continue
            seen.add(key)
            combined.append(item)
        return combined
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
