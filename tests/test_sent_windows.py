import csv
from datetime import datetime
from zoneinfo import ZoneInfo

import emailbot.messaging as messaging
from emailbot.messaging import get_sent_today, was_sent_within, clear_recent_sent_cache
from emailbot.messaging_utils import ensure_sent_log_schema
from emailbot.settings import REPORT_TZ


def _write_row(path: str, email: str, dt: datetime, status: str = "ok", source: str = "test", key: str = "k") -> None:
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        normalized = dt.replace(tzinfo=None)
        writer.writerow([key, email, normalized.isoformat(timespec="seconds"), source, status])


def test_today_and_180(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    log_path = tmp_path / "var" / "sent_log.csv"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(messaging, "LOG_FILE", str(log_path))
    messaging._log_cache = None
    clear_recent_sent_cache()

    ensure_sent_log_schema(str(log_path))
    tz = ZoneInfo(REPORT_TZ)
    now = datetime.now(tz)
    _write_row(str(log_path), "first.last@gmail.com", now)

    messaging._log_cache = None
    clear_recent_sent_cache()

    sent_today = get_sent_today()
    assert "first.last@gmail.com" in sent_today
    assert was_sent_within("first.last+tag@gmail.com", days=180) is True
