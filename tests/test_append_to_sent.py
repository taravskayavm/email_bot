from emailbot.messaging_utils import append_to_sent


class DummyImap:
    def __init__(self):
        self.append_calls = []

    def append(self, mailbox, flags, date_time, message):
        self.append_calls.append((mailbox, flags, date_time, message))
        return "OK", None


def test_append_to_sent_calls_imap_append():
    imap = DummyImap()
    ok, _ = append_to_sent(imap, "Sent", b"From: x\nTo: y\n\nBody")
    assert ok == "OK"
    assert imap.append_calls, "APPEND must be called exactly once"
    mb, flags, ts, msg = imap.append_calls[0]
    assert mb == "Sent"
    assert flags == "(\\Seen)"
    assert b"Body" in msg
