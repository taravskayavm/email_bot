from email.message import EmailMessage

from emailbot.messaging import was_sent_recently, mark_sent, _make_send_key


def _msg(ftu=("a@b.com", "c@d.com", "hi")):
    m = EmailMessage()
    m["From"] = ftu[0]
    m["To"] = ftu[1]
    m["Subject"] = ftu[2]
    return m


def test_was_sent_recently_flow(tmp_path, monkeypatch):
    # redirect storage file
    monkeypatch.setattr("emailbot.messaging.SENT_IDS_FILE", tmp_path / "sent_ids.jsonl")
    msg = _msg()
    assert was_sent_recently(msg) is False
    mark_sent(msg)
    # after marking, it should be True
    assert was_sent_recently(msg) is True


def test_make_key_is_stable_same_day():
    m1 = _msg(("a@b.com", "c@d.com", "s"))
    m2 = _msg(("a@b.com", "c@d.com", "s"))
    assert _make_send_key(m1) == _make_send_key(m2)

