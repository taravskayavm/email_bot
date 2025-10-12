from email.message import EmailMessage


def test_send_with_retry_smoke(monkeypatch):
    """Smoke test for send_with_retry logic using a dummy SMTP client."""

    monkeypatch.setenv("SMTP_JITTER_MIN_MS", "0")
    monkeypatch.setenv("SMTP_JITTER_MAX_MS", "0")

    from emailbot import smtp_client as sc

    calls = {"connect": 0, "ensure": 0, "send": 0, "close": 0}

    class DummySMTP(sc.RobustSMTP):
        def __init__(self):
            super().__init__()
            self._connected = False

        def connect(self):
            calls["connect"] += 1
            self._connected = True

            class _S:
                def noop(self_inner):
                    return None

            self._smtp = _S()

        def ensure(self):
            calls["ensure"] += 1
            if not self._connected:
                self.connect()

        def send(self, msg: EmailMessage):
            calls["send"] += 1
            return {"status": "ok", "to": msg.get_all("To", [])}

        def close(self):
            calls["close"] += 1
            self._connected = False
            self._smtp = None

    monkeypatch.setattr(sc, "RobustSMTP", DummySMTP)

    msg = EmailMessage()
    msg["From"] = "a@x.com"
    msg["To"] = "b@y.org"
    msg["Subject"] = "Hello"
    msg.set_content("Hi")

    smtp = DummySMTP()

    result = sc.send_with_retry(smtp=smtp, msg=msg, retries=1, backoff=0.1)

    assert isinstance(result, dict)
    assert result.get("status") == "ok"
    assert calls["send"] >= 1
