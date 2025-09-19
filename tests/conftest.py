"""Test configuration and shared fixtures."""

import sys
from pathlib import Path
from dataclasses import dataclass

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest

from utils import rules


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


@pytest.fixture(autouse=True)
def _isolated_history_db(tmp_path, monkeypatch):
    db_path = tmp_path / "history.db"
    monkeypatch.setenv("HISTORY_DB_PATH", str(db_path))
    # Reset lazy initialization between tests
    import emailbot.history_service as history_service
    import emailbot.history_store as history_store

    history_service._INITIALIZED_PATH = None
    history_store._INITIALIZED = False
    history_store._DB_PATH = db_path
    yield


@pytest.fixture(autouse=True)
def _isolated_rules_files(tmp_path, monkeypatch):
    history_path = tmp_path / "send_history.jsonl"
    blocklist_path = tmp_path / "blocklist.txt"
    monkeypatch.setattr(rules, "HISTORY_PATH", history_path)
    monkeypatch.setattr(rules, "BLOCKLIST_PATH", blocklist_path)
    rules.ensure_dirs()


@pytest.fixture(autouse=True)
def _isolated_send_stats(tmp_path, monkeypatch):
    stats_path = tmp_path / "send_stats.jsonl"
    audit_path = tmp_path / "audit.csv"
    sqlite_path = tmp_path / "send_history_cache.db"
    monkeypatch.setenv("SEND_STATS_PATH", str(stats_path))
    monkeypatch.setenv("AUDIT_PATH", str(audit_path))
    monkeypatch.setenv("APPEND_TO_SENT", "0")
    monkeypatch.setenv("SEND_HISTORY_SQLITE_PATH", str(sqlite_path))
