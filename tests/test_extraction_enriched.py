from datetime import datetime

from emailbot.extraction import extract_any_enriched
from emailbot.models import EmailEntry


def test_extract_any_enriched_pdf(monkeypatch):
    from emailbot import extraction as ex

    def fake_extract_any(path_or_url: str):
        assert path_or_url.endswith(".pdf")
        return ["a@b.com", "c@d.org"], {"dummy": True}

    monkeypatch.setattr(ex, "extract_any", fake_extract_any)

    timestamp = datetime.utcnow()
    result = extract_any_enriched(
        "sample.pdf",
        status="queued",
        last_sent=timestamp,
        meta={"fixture": True},
    )
    assert len(result) == 2
    assert isinstance(result[0], EmailEntry)
    assert result[0].source == "pdf"
    assert result[0].status == "queued"
    assert result[0].last_sent == timestamp
    assert result[0].meta.get("fixture") is True


def test_extract_any_enriched_url(monkeypatch):
    from emailbot import extraction as ex

    def fake_extract_any(path_or_url: str):
        assert path_or_url.startswith("https://")
        return ["x@y.com"], {"dummy": True}

    monkeypatch.setattr(ex, "extract_any", fake_extract_any)

    result = extract_any_enriched("https://example.com/page", meta={"where": "test"})
    assert len(result) == 1
    assert isinstance(result[0], EmailEntry)
    assert result[0].source == "url"
    assert result[0].status == "new"
    assert result[0].to_dict()["meta"]["where"] == "test"
