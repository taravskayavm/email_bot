from datetime import datetime

from emailbot.models import EmailEntry


def test_email_entry_basic():
    entry = EmailEntry.from_email("a@b.com", source="pdf")
    assert entry.email == "a@b.com"
    assert entry.source == "pdf"
    assert entry.status == "new"
    assert entry.last_sent is None
    assert isinstance(entry.meta, dict)


def test_email_entry_wrap_list_and_dict():
    meta = {"file": "foo.pdf"}
    items = EmailEntry.wrap_list(
        ["x@y.com", "z@k.org"],
        source="url",
        status="queued",
        meta=meta,
    )
    assert len(items) == 2
    assert items[0].status == "queued"
    assert items[0].meta is not meta
    items[0].meta["file"] = "bar.pdf"
    assert items[1].meta["file"] == "foo.pdf"
    data = items[0].to_dict()
    assert data["email"] == "x@y.com"
    assert data["source"] == "url"
    assert data["status"] == "queued"


def test_email_entry_datetime_serialization():
    now = datetime.utcnow()
    entry = EmailEntry(email="m@n.io", source="manual", last_sent=now)
    data = entry.to_dict()
    assert "last_sent" in data
    assert isinstance(data["last_sent"], str)
    parsed = datetime.fromisoformat(data["last_sent"])
    assert parsed.replace(microsecond=0) == now.replace(microsecond=0)
