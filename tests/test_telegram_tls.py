from __future__ import annotations

import asyncio
import ssl
from datetime import UTC

import httpx
import pytest
from telegram.error import NetworkError
from telegram.request import HTTPXRequest

from emailbot import telegram_network as net


@pytest.mark.parametrize('name', ['api.telegram.org', '*.telegram.org'])
def test_alternate_tls_requires_canonical_hostname_in_san(name):
    net._verify_telegram_hostname({'subjectAltName': [('DNS', name)]})


@pytest.mark.parametrize(
    'certificate',
    [
        {'subjectAltName': [('DNS', 'attacker.example')]},
        {'subjectAltName': [('DNS', 'www.api.telegram.org')]},
        {'subject': ((('commonName', 'api.telegram.org'),),)},
        {
            'subjectAltName': [('DNS', 'attacker.example')],
            'subject': ((('commonName', 'api.telegram.org'),),),
        },
        {},
    ],
)
def test_alternate_tls_rejects_wrong_and_missing_sans(certificate):
    with pytest.raises(ssl.CertificateError):
        net._verify_telegram_hostname(certificate)


def test_alternate_tls_keeps_ca_validation_and_changes_only_api_sni():
    context = net._TelegramSNIContext()
    assert context._normal.verify_mode == ssl.CERT_REQUIRED
    assert context._normal.check_hostname is True
    assert context._telegram.verify_mode == ssl.CERT_REQUIRED
    assert context._telegram.minimum_version >= ssl.TLSVersion.TLSv1_2
    obj = context.wrap_bio(ssl.MemoryBIO(), ssl.MemoryBIO(), server_hostname='api.telegram.org')
    assert isinstance(obj, net._TelegramSSLObject)
    assert obj.server_hostname == 'api.telegram.org.'
    other = context.wrap_bio(ssl.MemoryBIO(), ssl.MemoryBIO(), server_hostname='example.org')
    assert not isinstance(other, net._TelegramSSLObject)
    assert other.server_hostname == 'example.org'
    assert other.context.check_hostname is True


def test_hostname_check_happens_before_handshake_returns(monkeypatch):
    events = []
    monkeypatch.setattr(ssl.SSLObject, 'do_handshake', lambda self: events.append('chain'))
    monkeypatch.setattr(
        net._TelegramSSLObject,
        'getpeercert',
        lambda self: {'subjectAltName': [('DNS', 'attacker.example')]},
    )
    context = net._TelegramSNIContext()
    obj = context.wrap_bio(ssl.MemoryBIO(), ssl.MemoryBIO(), server_hostname='api.telegram.org')
    with pytest.raises(ssl.CertificateError):
        obj.do_handshake()
    assert events == ['chain']


def test_untrusted_certificate_never_reaches_hostname_check(monkeypatch):
    def fail_chain(self):
        raise ssl.SSLCertVerificationError('untrusted certificate')

    monkeypatch.setattr(ssl.SSLObject, 'do_handshake', fail_chain)
    monkeypatch.setattr(
        net, '_verify_telegram_hostname', lambda cert: pytest.fail('untrusted chain')
    )
    context = net._TelegramSNIContext()
    obj = context.wrap_bio(ssl.MemoryBIO(), ssl.MemoryBIO(), server_hostname='api.telegram.org')
    with pytest.raises(ssl.SSLCertVerificationError, match='untrusted certificate'):
        obj.do_handshake()


def test_tls_session_failure_recovers_same_route_and_remembers_client(monkeypatch):
    calls = []

    async def fake(self, *args, **kwargs):
        dotted = isinstance(self, net._TelegramSNIRequest)
        calls.append((net._request_route.get(), dotted, self.read_timeout))
        if not dotted:
            raise NetworkError('TLS failed') from httpx.ConnectError('[SSL: INVALID_SESSION_ID]')
        return 200, b'ok'

    monkeypatch.setattr(HTTPXRequest, 'do_request', fake)

    async def run():
        request = net.FailoverHTTPXRequest(('149.154.166.110',), read_timeout=37)
        async with request:
            for _ in range(2):
                assert await request.do_request(
                    'https://api.telegram.org/test/sendMessage', 'POST'
                ) == (200, b'ok')
            assert net._request_route.get() == ''
        assert all(r._client.is_closed for r in request._sni_requests.values())
        async with request:
            assert await request.do_request('https://api.telegram.org/test/getUpdates', 'POST') == (
                200,
                b'ok',
            )
        assert all(r._client.is_closed for r in request._sni_requests.values())

    asyncio.run(run())
    assert calls == [
        ('149.154.166.110', False, 37),
        ('149.154.166.110', True, 37),
        ('149.154.166.110', True, 37),
        ('149.154.166.110', True, 37),
    ]


@pytest.mark.parametrize(
    'url,error',
    [
        ('https://api.telegram.org/test/getMe', 'CERTIFICATE_VERIFY_FAILED'),
        ('https://api.telegram.org/test/getMe', 'connection refused'),
        ('https://example.org/test/getMe', 'INVALID_SESSION_ID'),
    ],
)
def test_sni_retry_is_limited_to_api_session_failure(monkeypatch, url, error):
    async def fake(self, *args, **kwargs):
        assert not isinstance(self, net._TelegramSNIRequest)
        raise NetworkError('connect failed') from httpx.ConnectError(error)

    monkeypatch.setattr(HTTPXRequest, 'do_request', fake)

    async def run():
        async with net.FailoverHTTPXRequest(('149.154.166.110',)) as request:
            with pytest.raises(NetworkError):
                await request.do_request(url, 'POST')
            assert request._sni_requests == {}
            assert net._request_route.get() == ''

    asyncio.run(run())


def test_failed_alternate_handshake_is_not_retried_forever(monkeypatch):
    calls = []

    async def fake(self, *args, **kwargs):
        calls.append((net._request_route.get(), isinstance(self, net._TelegramSNIRequest)))
        raise NetworkError('TLS failed') from httpx.ConnectError('INVALID_SESSION_ID')

    monkeypatch.setattr(HTTPXRequest, 'do_request', fake)

    async def run():
        async with net.FailoverHTTPXRequest(('149.154.166.110',)) as request:
            with pytest.raises(NetworkError):
                await request.do_request('https://api.telegram.org/test/getMe', 'POST')
            assert net._request_route.get() == ''

    asyncio.run(run())
    assert calls == [
        ('149.154.166.110', False),
        ('149.154.166.110', True),
        (None, False),
        (None, True),
    ]


@pytest.mark.parametrize(
    'hostname,trusted,valid',
    [
        ('api.telegram.org', True, True),
        ('attacker.example', True, False),
        ('api.telegram.org', False, False),
    ],
)
def test_real_tls_handshake_checks_chain_and_canonical_hostname(
    monkeypatch,
    tmp_path,
    hostname,
    trusted,
    valid,
):
    pytest.importorskip('cryptography')
    from datetime import datetime, timedelta

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hostname)])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(hostname)]), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_file = tmp_path / 'test-cert.pem'
    key_file = tmp_path / 'test-key.pem'
    cert_file.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )

    def verified_context():
        context = ssl.create_default_context()
        if trusted:
            context.load_verify_locations(cafile=str(cert_file))
        return context

    monkeypatch.setattr(httpx, 'create_ssl_context', verified_context)
    client_context = net._TelegramSNIContext()
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(cert_file, key_file)
    names = []
    server_context.set_servername_callback(lambda sock, name, context: names.append(name))
    client_in, client_out = ssl.MemoryBIO(), ssl.MemoryBIO()
    server_in, server_out = ssl.MemoryBIO(), ssl.MemoryBIO()
    client = client_context.wrap_bio(client_in, client_out, server_hostname='api.telegram.org')
    server = server_context.wrap_bio(server_in, server_out, server_side=True)

    def handshake():
        client_done = server_done = False
        for _ in range(20):
            if not client_done:
                try:
                    client.do_handshake()
                    client_done = True
                except ssl.SSLWantReadError:
                    pass
            data = client_out.read()
            if data:
                server_in.write(data)
            if not server_done:
                try:
                    server.do_handshake()
                    server_done = True
                except ssl.SSLWantReadError:
                    pass
            data = server_out.read()
            if data:
                client_in.write(data)
            if client_done and server_done:
                return
        pytest.fail('TLS handshake did not complete')

    if valid:
        handshake()
    else:
        with pytest.raises(ssl.SSLCertVerificationError):
            handshake()
    assert names == ['api.telegram.org.']
