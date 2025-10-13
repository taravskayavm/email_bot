"""Utilities for establishing IMAP connections with additional policies."""

from __future__ import annotations

import os
import socket
import ssl
import imaplib
from typing import Optional


def _resolve_preferred_addrinfos(host: str, port: int) -> list[tuple]:
    """Return address infos preferring IPv4 when ``IMAP_IPV4_ONLY`` is set."""

    prefer_ipv4 = os.getenv("IMAP_IPV4_ONLY", "0") == "1"
    addrinfos = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    if prefer_ipv4:
        only_v4 = [info for info in addrinfos if info[0] == socket.AF_INET]
        if only_v4:
            return only_v4
    return addrinfos


def get_imap_timeout(default: Optional[float] = None) -> Optional[float]:
    """Return the IMAP timeout from the ``IMAP_TIMEOUT`` env var."""

    raw = os.getenv("IMAP_TIMEOUT")
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def imap_connect_ssl(
    host: str, port: int, timeout: Optional[float] = None
) -> imaplib.IMAP4_SSL:
    """Create an ``IMAP4_SSL`` connection honouring IPv4 preference and timeout."""

    context = ssl.create_default_context()
    last_error: Optional[BaseException] = None
    for family, socktype, proto, _, sockaddr in _resolve_preferred_addrinfos(host, port):
        sock = None
        wrapped = None
        try:
            sock = socket.socket(family, socktype, proto)
            if timeout is not None:
                sock.settimeout(timeout)
            wrapped = context.wrap_socket(sock, server_hostname=host)
            wrapped.connect(sockaddr)
            try:
                return imaplib.IMAP4_SSL(
                    host=None, port=None, ssl_context=context, sock=wrapped
                )
            except TypeError:
                # Совместимость: старые версии imaplib не принимают параметр sock
                try:
                    wrapped.close()
                except Exception:
                    pass
                try:
                    sock.close()
                except Exception:
                    pass
                sock = None
                wrapped = None
                client = imaplib.IMAP4_SSL(host=host, port=port)
                if timeout is not None:
                    try:
                        client.sock.settimeout(timeout)
                    except Exception:
                        pass
                return client
        except Exception as exc:  # pragma: no cover - network errors
            last_error = exc
            if wrapped is not None:
                try:
                    wrapped.close()
                except Exception:
                    pass
            elif sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass
    if last_error is not None:
        raise last_error
    raise RuntimeError("IMAP connect failed")
