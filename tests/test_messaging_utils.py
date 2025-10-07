import csv
from datetime import datetime

from emailbot import messaging_utils as mu, messaging


def setup_paths(tmp_path, monkeypatch):
    suppress = tmp_path / "s.csv"
    bounce = tmp_path / "b.csv"
    monkeypatch.setattr(mu, "SUPPRESS_PATH", suppress)
    monkeypatch.setattr(mu, "BOUNCE_LOG_PATH", bounce)
    return suppress, bounce


def test_suppress_add_and_is_suppressed(tmp_path, monkeypatch):
    s, _ = setup_paths(tmp_path, monkeypatch)
    mu.suppress_add("user@example.com", 550, "hard")
    mu.suppress_add("user@example.com", 550, "hard")
    assert mu.is_suppressed("user@example.com") is True
    with s.open() as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["hits"] == "2"


def test_add_bounce_writes_log(tmp_path, monkeypatch):
    _, b = setup_paths(tmp_path, monkeypatch)
    mu.add_bounce("a@b.com", 550, "user unknown", "send")
    with b.open() as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["email"] == "a@b.com"
    assert rows[0]["code"] == "550"
    assert rows[0]["phase"] == "send"


def test_is_hard_bounce():
    assert mu.is_hard_bounce(550, "err")
    assert not mu.is_hard_bounce(450, "err")
    assert mu.is_hard_bounce(None, "User Not Found")
    assert not mu.is_hard_bounce(None, "temporary failure")


def test_is_soft_bounce():
    assert mu.is_soft_bounce(450, "temporary failure")
    assert mu.is_soft_bounce(None, "greylisted")
    assert not mu.is_soft_bounce(550, "User not found")
    assert not mu.is_soft_bounce(None, "permanent error")


def test_bounce_code_parsing():
    assert mu.is_soft_bounce(None, "451 try again later")
    assert mu.is_hard_bounce(None, "550 user unknown")


def test_gmail_canonicalization_for_180_days(tmp_path, monkeypatch):
    log = tmp_path / "log.csv"
    monkeypatch.setattr(messaging, "LOG_FILE", str(log))
    from emailbot import history_service
    from datetime import datetime, timezone

    history_service.mark_sent(
        "user.name+tag@gmail.com",
        "",
        None,
        datetime.now(timezone.utc),
    )
    assert mu.was_sent_within("username@gmail.com") is True


def test_canonical_for_history_gmail_variants():
    assert mu.canonical_for_history("User.Name+tag@googlemail.com") == mu.canonical_for_history(
        "username@gmail.com"
    )


def test_schema_migration_from_legacy_headers(tmp_path):
    path = tmp_path / "sent_log.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["address", "timestamp"])
        w.writerow(["User@Example.com", "2023-01-01"])
    fields = mu.ensure_sent_log_schema(str(path))
    assert fields[:5] == mu.REQUIRED_FIELDS
    with path.open() as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == mu.REQUIRED_FIELDS
        rows = list(reader)
    assert rows[0]["last_sent_at"] == "2023-01-01T00:00:00+00:00"
    assert rows[0]["email"] == "User@Example.com"


def test_upsert_idempotent(tmp_path):
    path = tmp_path / "sent_log.csv"
    ts = datetime(2023, 1, 1)
    ins, upd = mu.upsert_sent_log(path, "Test@Example.com", ts, "src", key="k1")
    assert (ins, upd) == (True, False)
    ins2, upd2 = mu.upsert_sent_log(path, "test@example.com", ts, "src", key="k1")
    assert (ins2, upd2) == (False, True)
    ins3, upd3 = mu.upsert_sent_log(path, "test@example.com", ts, "src", key="k2")
    assert (ins3, upd3) == (True, False)
    with path.open() as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2


def test_classify_tld():
    assert mu.classify_tld("user@gmail.com") == "generic"
    assert mu.classify_tld("user@ncfu.ru") == "domestic"
    assert mu.classify_tld("user@gmail.com.br") == "foreign"
    assert mu.classify_tld("user@edu.br") == "foreign"
    assert mu.classify_tld("user@ufjf.br") == "foreign"
    assert mu.classify_tld("user@ufmg.br") == "foreign"
    assert mu.classify_tld("user@ufop.edu.br") == "foreign"
