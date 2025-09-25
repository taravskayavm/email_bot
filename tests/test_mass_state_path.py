import os, json, tempfile, importlib


def test_mass_state_uses_env_path(monkeypatch, tmp_path):
    p = tmp_path / "ms.json"
    monkeypatch.setenv("MASS_STATE_PATH", str(p))
    mod = importlib.import_module("emailbot.mass_state")
    state = {"x": 1}
    mod._state_cache = {"42": state}
    mod._save_all(state)  # type: ignore[arg-type]
    data = json.loads(p.read_text(encoding="utf-8"))
    assert data["42"]["x"] == 1
