import os
import json
from pathlib import Path

import pytest

from emailbot.messaging import send_raw_smtp_with_retry


class _FakeSMTP:
    def __init__(self, *a, **k):
        pass

    def send(self, msg):
        return None

    def ensure(self):
        return None

    def close(self):
        return None


@pytest.fixture(autouse=True)
def _env_tmp_stats(tmp_path, monkeypatch):
    stats = tmp_path / "send_stats.jsonl"
    monkeypatch.setenv("SEND_STATS_PATH", str(stats))
    # подменяем SMTP-клиент на фейковый
    import emailbot.messaging as m
    fake = _FakeSMTP
    monkeypatch.setattr(m, "RobustSMTP", lambda *a, **k: fake(), raising=True)
    monkeypatch.setattr(
        m,
        "send_with_retry",
        lambda smtp, msg, *, retries=2, backoff=1.0: smtp.send(msg),
    )
    yield


def _build_raw(group="sport", title=None):
    from email.message import EmailMessage

    msg = EmailMessage()
    msg["From"] = "a@b.ru"
    msg["To"] = "c@d.ru"
    msg["Subject"] = "t"
    if group:
        title_value = title if title is not None else group.upper()
        msg["X-EBOT-Group"] = title_value
        msg["X-EBOT-Group-Key"] = group
    msg.set_content("hi")
    return msg.as_bytes()


def test_success_is_logged(tmp_path):
    raw = _build_raw("sport", title="Спорт")
    send_raw_smtp_with_retry(raw, "c@d.ru", max_tries=1)
    stats = Path(os.environ["SEND_STATS_PATH"])
    lines = stats.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["status"] == "success"
    assert rec["email"] == "c@d.ru"
    assert rec.get("group") == "sport"
