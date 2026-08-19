from emailbot import messaging


class _FakeIMAP:
    def __init__(self, msg_bytes):
        self._msg = msg_bytes
        self.store_calls = []
        self.fetch_specs = []
        self.search_calls = []

    def search(self, *args, **kwargs):
        self.search_calls.append(args)
        return 'OK', [b'1']

    def fetch(self, *args, **kwargs):
        self.fetch_specs.append(args)
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
    assert imap1.fetch_specs == [(b"1", "(BODY.PEEK[])")]
    assert any(flag == '+FLAGS' and val == '\\Seen' for _, flag, val in imap1.store_calls)

    msg2 = _build_msg('Имя без адреса', 'unsubscribe')
    imap2 = _FakeIMAP(msg2)
    assert messaging.process_unsubscribe_requests(imap2) == 0
    assert imap2.fetch_specs == [(b"1", "(BODY.PEEK[])")]
    assert not imap2.store_calls


def test_quoted_unsubscribe_footer_does_not_unsubscribe_sender(tmp_path, monkeypatch):
    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(tmp_path / "blocked_emails.txt"))
    msg = _build_msg(
        "Regular sender <regular@example.com>",
        "Обычная тема",
        "Добрый день!\n\n> Отписаться: mailto:sender@example.com?subject=unsubscribe",
    )
    imap = _FakeIMAP(msg)

    assert messaging.process_unsubscribe_requests(imap) == 0
    assert not imap.store_calls


def test_explicit_russian_body_command_unsubscribes(tmp_path, monkeypatch):
    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(tmp_path / "blocked_emails.txt"))
    msg = _build_msg(
        "User <russian@example.com>",
        "Без темы",
        "Прошу отписать этот адрес от рассылки.\nАдрес получателя: russian@example.com",
    )
    imap = _FakeIMAP(msg)

    assert messaging.process_unsubscribe_requests(imap) == 1
    assert imap.store_calls


def test_legacy_russian_subject_has_narrow_imap_search(tmp_path, monkeypatch):
    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(tmp_path / "blocked_emails.txt"))
    imap = _FakeIMAP(
        _build_msg(
            "Artur <artur.nesterenko@hotmail.com>",
            "Отписка от рассылки",
            "Прошу отписать этот адрес от рассылки.",
        )
    )

    assert messaging.process_unsubscribe_requests(imap) == 1
    assert any(call[0] == "UTF-8" for call in imap.search_calls)
    assert all(b"UNSEEN" not in str(call).encode() for call in imap.search_calls)


def test_already_blocked_unsubscribe_is_not_reprocessed(tmp_path, monkeypatch):
    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(tmp_path / "blocked_emails.txt"))
    monkeypatch.setattr(messaging, "_is_blocklisted", lambda _email: True)
    monkeypatch.setattr(
        messaging,
        "mark_unsubscribed",
        lambda _email: (_ for _ in ()).throw(AssertionError("must not run")),
    )
    imap = _FakeIMAP(_build_msg("User <blocked@example.com>", "unsubscribe"))

    assert messaging.process_unsubscribe_requests(imap) == 0
    assert not imap.store_calls
