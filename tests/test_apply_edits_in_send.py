from emailbot.messaging_utils import prepare_recipients_for_send


def test_trailing_cyrillic_tail_is_removed(monkeypatch):
    monkeypatch.setattr(
        "emailbot.edit_service.load_edits",
        lambda: {"MAP": {}, "DROP": set()},
    )
    good, dropped, remap = prepare_recipients_for_send(["user@example.ru кафедра"])
    assert good == ["user@example.ru"]
    assert not dropped
    assert remap["user@example.ru кафедра"] == "user@example.ru"


def test_edits_drop_and_map(monkeypatch):
    monkeypatch.setattr(
        "emailbot.edit_service.load_edits",
        lambda: {
            "DROP": ["bad@host.tld"],
            "MAP": {"wrong@host.tld": "right@host.tld"},
        },
    )
    good, dropped, remap = prepare_recipients_for_send(
        ["bad@host.tld", "wrong@host.tld"]
    )
    assert "bad@host.tld" in dropped
    assert good == ["right@host.tld"]
    assert remap["wrong@host.tld"] == "right@host.tld"
