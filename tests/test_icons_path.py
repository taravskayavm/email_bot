import json
from importlib import reload
from pathlib import Path


def test_icons_loaded_from_module_dir(tmp_path, monkeypatch):
    # подложим icons.json рядом с модулем
    import emailbot.bot.keyboards as kb

    icons_path = Path(kb.__file__).resolve().parent / "icons.json"
    backup = None
    if icons_path.exists():
        backup = icons_path.read_bytes()
    try:
        icons_path.write_text(json.dumps({"bioinformatics": "🧬"}), encoding="utf-8")
        reload(kb)
        # _load_icons должен вернуть наши данные
        icons = kb._load_icons()
        assert icons.get("bioinformatics") == "🧬"
    finally:
        if backup is None:
            icons_path.unlink(missing_ok=True)
        else:
            icons_path.write_bytes(backup)
