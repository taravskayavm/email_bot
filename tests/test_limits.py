import types
import urllib.request

from emailbot import extraction_url


class DummyHeaders:
    def get_content_charset(self):
        return 'utf-8'


class DummyResp:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0
        self.read_calls = 0

    def geturl(self):
        return 'http://example.com'

    @property
    def headers(self):
        return DummyHeaders()

    def read(self, n: int) -> bytes:
        self.read_calls += 1
        if self.pos >= len(self.data):
            return b''
        chunk = self.data[self.pos:self.pos + n]
        self.pos += n
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_http_timeout(monkeypatch):
    captured = {}

    def fake_urlopen(req, timeout=None):
        captured['timeout'] = timeout
        raise TimeoutError

    monkeypatch.setattr(urllib.request, 'urlopen', fake_urlopen)
    assert extraction_url.fetch_url('http://example.com', timeout=1) is None
    assert captured['timeout'] == 1


def test_max_size_limit(monkeypatch):
    resp = DummyResp(b'x' * 40)
    monkeypatch.setattr(urllib.request, 'urlopen', lambda req, timeout=None: resp)
    monkeypatch.setattr(extraction_url, '_READ_CHUNK', 10)
    text = extraction_url.fetch_url('http://example.com', max_size=15)
    assert len(text) <= 20
    assert resp.read_calls == 2


def test_stop_event(monkeypatch):
    resp = DummyResp(b'x' * 40)
    monkeypatch.setattr(urllib.request, 'urlopen', lambda req, timeout=None: resp)
    extraction_url._CACHE.clear()

    class Stop:
        def is_set(self):
            return True

    assert extraction_url.fetch_url('http://example.org', stop_event=Stop()) is None
    assert resp.read_calls == 0
