"""Regression tests for the single-page-only URL parsing policy."""

import asyncio
import time

import pipelines.extract_emails as url_pipeline
from emailbot.pipelines import ingest_url as ingest_module


def test_deep_flag_cannot_start_crawler(monkeypatch):
    """Legacy ``deep=True`` callers must still fetch exactly one page."""

    fetched: list[str] = []

    def fake_get(url: str, **_kwargs) -> str:
        fetched.append(url)
        return "contact: page@example.com"

    def fail_crawler(*_args, **_kwargs):
        raise AssertionError("site crawler must not be constructed")

    monkeypatch.setattr(url_pipeline, "_http_get_text", fake_get)
    monkeypatch.setattr(url_pipeline, "Crawler", fail_crawler)
    monkeypatch.setattr(
        url_pipeline,
        "extract_emails_pipeline",
        lambda _html: (["page@example.com"], {"found_raw": 1}),
    )

    emails, stats = asyncio.run(
        url_pipeline.extract_from_url_async(
            "https://example.com/contact",
            deep=True,
            path_prefixes=["/staff"],
            max_pages=100,
        )
    )

    assert fetched == ["https://example.com/contact"]
    assert emails == ["page@example.com"]
    assert stats["pages"] == 1
    assert stats["page_urls"] == ["https://example.com/contact"]
    assert "pages_limit" not in stats
    assert "path_prefixes" not in stats


def test_ingest_url_never_forwards_crawl_options(monkeypatch):
    """The public ingest service enforces the same policy at its boundary."""

    received: dict[str, object] = {}

    async def fake_extract(url: str, **kwargs):
        received["url"] = url
        received.update(kwargs)
        return ["page@example.com"], {"pages": 1, "found_raw": 1}

    monkeypatch.setattr(ingest_module, "extract_from_url_async", fake_extract)
    monkeypatch.setattr(ingest_module, "count_blocked", lambda _emails: 0)

    emails, stats = asyncio.run(
        ingest_module.ingest_url(
            "https://example.com/contact",
            deep=True,
            path_prefixes=["/staff"],
            limit_pages=100,
        )
    )

    assert received == {
        "url": "https://example.com/contact",
        "deep": False,
        "path_prefixes": None,
        "max_pages": None,
    }
    assert emails == ["page@example.com"]
    assert stats["pages"] == 1


def test_url_fetch_does_not_block_event_loop(monkeypatch):
    """A slow synchronous HTTP request must not pause other bot updates."""

    events: list[str] = []

    def slow_get(_url: str, **_kwargs) -> str:
        time.sleep(0.08)
        events.append("fetch")
        return "contact: page@example.com"

    async def heartbeat() -> None:
        await asyncio.sleep(0.01)
        events.append("heartbeat")

    monkeypatch.setattr(url_pipeline, "_http_get_text", slow_get)
    monkeypatch.setattr(
        url_pipeline,
        "extract_emails_pipeline",
        lambda _html: (["page@example.com"], {"found_raw": 1}),
    )

    async def run_both():
        return await asyncio.gather(
            url_pipeline.extract_from_url_async("https://example.com/contact"),
            heartbeat(),
        )

    asyncio.run(run_both())

    assert events == ["heartbeat", "fetch"]


def test_streamed_url_response_is_rejected_at_limit():
    """The response reader must stop before a large page fills memory."""

    class FakeResponse:
        headers = {}

        @staticmethod
        def iter_bytes():
            yield b"1234"
            yield b"5678"

    try:
        url_pipeline._read_limited_response(FakeResponse(), 6)
    except url_pipeline.URLResponseTooLargeError:
        pass
    else:
        raise AssertionError("oversized response was accepted")
