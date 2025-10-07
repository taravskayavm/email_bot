import asyncio

from crawler.web_crawler import Crawler


def test_crawler_has_head_method():
    crawler = Crawler("https://example.com")
    try:
        assert hasattr(crawler, "_passes_head_filter")
    finally:
        asyncio.run(crawler.close())
