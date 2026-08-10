"""Tests for concurrent local-file parsing helpers."""

import threading
import time

import emailbot.parallel_parse as parallel_parse


def test_results_are_collected_in_completion_order(monkeypatch):
    release_slow = threading.Event()
    fast_finished = threading.Event()

    monkeypatch.setattr(parallel_parse, "PARSE_MAX_WORKERS", 2)
    monkeypatch.setattr(parallel_parse, "PARSE_FILE_TIMEOUT", 2)

    def worker(path: str) -> str:
        if path == "slow.pdf":
            assert release_slow.wait(timeout=1)
            time.sleep(0.05)
        else:
            fast_finished.set()
            release_slow.set()
        return path

    results = parallel_parse.parallel_map_files(
        ["slow.pdf", "fast.txt"], worker
    )

    assert fast_finished.is_set()
    assert results == ["fast.txt", "slow.pdf"]


def test_worker_failure_does_not_discard_successes(monkeypatch):
    monkeypatch.setattr(parallel_parse, "PARSE_MAX_WORKERS", 2)
    monkeypatch.setattr(parallel_parse, "PARSE_FILE_TIMEOUT", 2)

    def worker(path: str) -> str:
        if path == "broken.pdf":
            raise ValueError("cannot parse")
        return path

    results = parallel_parse.parallel_map_files(
        ["broken.pdf", "valid.txt"], worker
    )

    assert results == ["valid.txt"]
