"""Test configuration and shared fixtures."""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest
from dataclasses import dataclass


@dataclass
class _Resp:
    text: str = ""
    content: bytes = b""
    url: str | None = None
    encoding: str = "utf-8"

    def iter_bytes(self, chunk_size: int = 65536):
        yield self.content


def _mk_resp(data: bytes | str, url: str | None = None) -> _Resp:
    if isinstance(data, bytes):
        try:
            text = data.decode("utf-8")
        except Exception:
            text = ""
        return _Resp(text=text, content=data, url=url)
    else:
        return _Resp(text=data, content=data.encode("utf-8"), url=url)


@pytest.fixture
def make_fetch():
    def factory(mapping: dict[str, bytes | str | Path]):
        def fetch(url: str) -> _Resp:
            data = mapping[url]
            if isinstance(data, Path):
                return _mk_resp(data.read_bytes(), url)
            return _mk_resp(data, url)
        return fetch
    return factory


@pytest.fixture
def httpx_file_server(monkeypatch):
    def factory(mapping: dict[str, Path]):
        def fake_get(url: str, *args, **kwargs):
            path = mapping[url]
            return _mk_resp(path.read_bytes(), url)

        def fake_stream(method: str, url: str, *args, **kwargs):
            resp = fake_get(url)

            class _CM:
                def __enter__(self_inner):
                    return resp

                def __exit__(self_inner, exc_type, exc, tb):
                    return False

            return _CM()

        monkeypatch.setattr("httpx.get", fake_get)
        monkeypatch.setattr("httpx.stream", fake_stream)
    return factory
