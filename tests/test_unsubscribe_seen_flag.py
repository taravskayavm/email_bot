from emailbot import messaging


class _FakeIMAP:
    def __init__(self, msg_bytes):
        self._msg = msg_bytes
        self.store_calls = []

    def search(self, *args, **kwargs):
        return 'OK', [b'1']

    def fetch(self, *args, **kwargs):
        return 'OK', [(None, self._msg)]

    def store(self, num, flags, value):
        self.store_calls.append((num, flags, value))
        return 'OK', []


def _build_msg(from_header: str, subject: str, body: str = "unsubscribe"):
    from email.message import EmailMessage

    m = EmailMessage()
    m['From'] = from_header
    m['Subject'] = subject
    m.set_content(body)
    return m.as_bytes()


def test_unsubscribe_marks_seen_only_on_success(tmp_path, monkeypatch):
    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(tmp_path / "blocked_emails.txt"))

    msg1 = _build_msg('User <user@example.com>', 'unsubscribe')
    imap1 = _FakeIMAP(msg1)
    assert messaging.process_unsubscribe_requests(imap1) == 1
    assert any(flag == '+FLAGS' and val == '\\Seen' for _, flag, val in imap1.store_calls)

    msg2 = _build_msg('Имя без адреса', 'unsubscribe')
    imap2 = _FakeIMAP(msg2)
    assert messaging.process_unsubscribe_requests(imap2) == 0
    assert not imap2.store_calls
