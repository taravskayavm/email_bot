import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest

from emailbot import messaging_utils


def test_concurrent_upserts_preserve_every_recipient(tmp_path):
    path = tmp_path / "sent_log.csv"

    def write(index: int) -> None:
        messaging_utils.upsert_sent_log(
            path,
            f"user{index}@example.com",
            datetime.now(timezone.utc),
            "test",
            status="ok",
            key=f"event-{index}",
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(write, range(16)))

    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert {row["email"] for row in rows} == {
        f"user{index}@example.com" for index in range(16)
    }


def test_failed_atomic_write_does_not_restore_stale_backup(tmp_path, monkeypatch):
    path = tmp_path / "sent_log.csv"
    messaging_utils.upsert_sent_log(
        path,
        "existing@example.com",
        datetime.now(timezone.utc),
        "test",
        status="ok",
        key="existing-event",
    )
    original = path.read_bytes()
    path.with_suffix(".csv.bak").write_text(
        "key,email,last_sent_at,source,status\n"
        "stale,stale@example.com,2020-01-01T00:00:00+00:00,test,ok\n",
        encoding="utf-8",
    )

    def fail_write(*args, **kwargs):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(messaging_utils, "_atomic_write", fail_write)

    with pytest.raises(OSError, match="simulated replace failure"):
        messaging_utils.upsert_sent_log(
            path,
            "new@example.com",
            datetime.now(timezone.utc),
            "test",
            status="ok",
            key="new-event",
        )

    assert path.read_bytes() == original
