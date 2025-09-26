from importlib import reload


def test_get_sender_returns_singleton(monkeypatch):
    """Ensure the messaging sender singleton is stable."""

    import emailbot.aiogram_port.messaging as messaging

    reload(messaging)

    class DummySender:
        def __init__(self):
            self.ok = True

    monkeypatch.setattr(messaging, "SmtpSender", DummySender, raising=True)
    monkeypatch.setattr(messaging, "_SENDER", None, raising=False)

    s1 = messaging._get_sender()
    s2 = messaging._get_sender()
    assert s1 is s2
    assert getattr(s1, "ok", False) is True
