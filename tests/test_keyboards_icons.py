import json
from importlib import reload


def test_icons_loaded_when_file_exists(tmp_path, monkeypatch):
    """Icons should load from a provided JSON file regardless of CWD."""

    icons = {"pdf": "📄", "email": "✉️"}
    path = tmp_path / "icons.json"
    path.write_text(json.dumps(icons, ensure_ascii=False), encoding="utf-8")

    import emailbot.bot.keyboards as keyboards

    reload(keyboards)
    monkeypatch.delenv("DIRECTION_ICONS_JSON", raising=False)
    monkeypatch.setattr(keyboards, "ICONS_PATH", path, raising=True)

    loaded = keyboards._load_icons()
    assert loaded == icons
