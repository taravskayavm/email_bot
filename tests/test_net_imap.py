import socket
from types import SimpleNamespace

import pytest

from emailbot import net_imap


def test_legacy_imap_greeting_has_timeout(monkeypatch):
    """A server that stalls before its greeting must not block the poller."""
    timeout = 0.25
    connections = []

    class Transport:
        def __init__(self, current_timeout=None):
            self.current_timeout = current_timeout
            self.closed = False

        def settimeout(self, value):
            self.current_timeout = value

        def connect(self, address):
            pass

        def makefile(self, mode):
            return self

        def readline(self, *args):
            assert self.current_timeout == timeout, "IMAP greeting has no timeout"
            raise TimeoutError("server did not send its greeting")

        def close(self):
            self.closed = True

        def shutdown(self, how):
            pass

    context = SimpleNamespace(wrap_socket=lambda transport, **kwargs: transport)
    probe = Transport()

    def create_connection(address, timeout=None, *args, **kwargs):
        transport = Transport(timeout)
        connections.append(transport)
        return transport

    monkeypatch.setattr(net_imap.ssl, "create_default_context", lambda: context)
    monkeypatch.setattr(net_imap.ssl, "_create_stdlib_context", lambda **kwargs: context)
    monkeypatch.setattr(net_imap.socket, "socket", lambda *args: probe)
    monkeypatch.setattr(net_imap.socket, "create_connection", create_connection)
    monkeypatch.setattr(
        net_imap,
        "_resolve_preferred_addrinfos",
        lambda *args: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 993))],
    )

    with pytest.raises(TimeoutError, match="server did not send its greeting"):
        net_imap.imap_connect_ssl("imap.example.com", 993, timeout)

    assert probe.closed
    assert len(connections) == 1


def test_legacy_imap_preserves_tls_context_and_timeout(monkeypatch):
    class Transport:
        def settimeout(self, value):
            pass

        def connect(self, address):
            pass

        def close(self):
            pass

    transport = Transport()
    context = SimpleNamespace(wrap_socket=lambda *args, **kwargs: transport)
    client = SimpleNamespace(sock=transport)
    calls = []

    # This signature matches Python 3.11: passing sock raises TypeError.
    def legacy_imap(host, port, ssl_context=None, timeout=None):
        calls.append((host, port, ssl_context, timeout))
        return client

    monkeypatch.setattr(net_imap.ssl, "create_default_context", lambda: context)
    monkeypatch.setattr(net_imap.socket, "socket", lambda *args: transport)
    monkeypatch.setattr(net_imap.imaplib, "IMAP4_SSL", legacy_imap)
    monkeypatch.setattr(
        net_imap,
        "_resolve_preferred_addrinfos",
        lambda *args: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 993))],
    )

    assert net_imap.imap_connect_ssl("imap.example.com", 993, 15) is client
    assert calls == [("imap.example.com", 993, context, 15)]
