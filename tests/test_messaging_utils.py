import csv
from emailbot import messaging_utils as mu


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
