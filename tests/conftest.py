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


def _mk_resp(data: bytes | str) -> _Resp:
    if isinstance(data, bytes):
        try:
            text = data.decode("utf-8")
        except Exception:
            text = ""
        return _Resp(text=text, content=data)
    else:
        return _Resp(text=data, content=data.encode("utf-8"))


@pytest.fixture
def make_fetch():
    def factory(mapping: dict[str, bytes | str | Path]):
        def fetch(url: str) -> _Resp:
            data = mapping[url]
            if isinstance(data, Path):
                return _mk_resp(data.read_bytes())
            return _mk_resp(data)
        return fetch
    return factory
