from services.templates import list_templates


def test_discovery_honors_exts_and_labels(tmp_path, monkeypatch):
    base = tmp_path / "templates"
    base.mkdir()
    (base / "sport.html").write_text("x", encoding="utf-8")
    (base / "_labels.json").write_text('{"sport":"Спорт"}', encoding="utf-8")
    monkeypatch.setenv("TEMPLATES_DIR", str(base))
    res = list_templates()
    assert any(x["label"] == "Спорт" and x["code"] == "sport" for x in res)
