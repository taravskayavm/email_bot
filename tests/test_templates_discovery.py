from services.templates import list_templates


def test_discovery_honors_exts_and_labels(tmp_path, monkeypatch):
    base = tmp_path / "templates"
    base.mkdir()
    (base / "sport.html").write_text("x", encoding="utf-8")
    (base / "extra.html").write_text("x", encoding="utf-8")
    (base / "_labels.json").write_text('{"sport":"Спорт"}', encoding="utf-8")
    monkeypatch.setenv("TEMPLATES_DIR", str(base))
    res = list_templates()
    assert any(x["label"] == "Спорт" and x["code"] == "sport" for x in res)
    assert all(x["code"] != "extra" for x in res)


def test_dict_metadata_used(tmp_path, monkeypatch):
    base = tmp_path / "templates"
    base.mkdir()
    (base / "sport.html").write_text("x", encoding="utf-8")
    (base / "_labels.json").write_text(
        '{"sport": {"title": "Super Sport", "signature": "new"}}', encoding="utf-8"
    )
    monkeypatch.setenv("TEMPLATES_DIR", str(base))

    res = list_templates()
    assert res == [
        {
            "code": "sport",
            "label": "Super Sport",
            "signature": "new",
            "path": str((base / "sport.html").resolve()),
        }
    ]


def test_missing_template_file_skipped(tmp_path, monkeypatch):
    base = tmp_path / "templates"
    base.mkdir()
    (base / "_labels.json").write_text('{"ghost": {"title": "Ghost"}}', encoding="utf-8")
    monkeypatch.setenv("TEMPLATES_DIR", str(base))

    assert list_templates() == []
