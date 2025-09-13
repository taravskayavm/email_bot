def test_merge_suspects_into_sendable():
    user_data = {
        "emails_for_sending": ["ok1@mail.ru", "ok2@mail.ru"],
        "emails_suspects": ["alex@mail.ru", "boris@yandex.ru"],
    }
    sendable = set(user_data.get("emails_for_sending") or [])
    for e in user_data.get("emails_suspects") or []:
        sendable.add(e)
    user_data["emails_for_sending"] = sorted(sendable)
    user_data["emails_suspects"] = []
    assert "alex@mail.ru" in user_data["emails_for_sending"]
    assert "boris@yandex.ru" in user_data["emails_for_sending"]
    assert user_data["emails_suspects"] == []
