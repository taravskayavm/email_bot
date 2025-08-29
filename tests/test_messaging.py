import csv
import sys
from pathlib import Path

import pytest

# Ensure package root on path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from emailbot import messaging


@pytest.fixture(autouse=True)
def fake_smtp(monkeypatch):
    class DummySmtp:
        def __init__(self, *a, **kw):
            pass
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            pass
        def send(self, *a, **kw):
            pass
    monkeypatch.setattr(messaging, "SmtpClient", DummySmtp)


@pytest.fixture
def temp_files(tmp_path, monkeypatch):
    blocked = tmp_path / "blocked.txt"
    log = tmp_path / "logs" / "sent_log.csv"
    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(blocked))
    monkeypatch.setattr(messaging, "LOG_FILE", str(log))
    return blocked, log


def test_add_blocked_email_handles_duplicates_and_invalid(temp_files):
    blocked, _ = temp_files
    # invalid email
    assert messaging.add_blocked_email("invalid") is False
    assert not blocked.exists()

    # add new email
    assert messaging.add_blocked_email("User@Example.COM ") is True
    assert blocked.read_text().splitlines() == ["user@example.com"]

    # duplicate should not be added
    assert messaging.add_blocked_email("user@example.com") is False
    assert blocked.read_text().splitlines() == ["user@example.com"]


def test_dedupe_blocked_file_removes_duplicates_and_variants(temp_files):
    blocked, _ = temp_files
    blocked.write_text(
        "\n".join([
            "john@example.com",
            "John@example.com",
            "1john@example.com",
            "2john@example.com",
            "1jane@example.com",
            "1john@example.com",
        ])
        + "\n"
    )
    messaging.dedupe_blocked_file()
    result = blocked.read_text().splitlines()
    assert result == ["1jane@example.com", "john@example.com"]


def test_log_sent_email_records_entries(temp_files):
    _, log_path = temp_files
    messaging.log_sent_email("USER@example.com", "group1")
    messaging.log_sent_email(
        "USER@example.com", "group1", status="error", error_msg="boom"
    )
    with open(log_path, encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert len(rows) == 2
    assert rows[0][1:4] == ["user@example.com", "group1", "ok"]
    assert rows[1][3] == "error" and rows[1][6] == "boom"
