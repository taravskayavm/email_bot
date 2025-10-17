# -*- coding: utf-8 -*-
"""High-level crawling helpers to extract e-mails from a site.

Supports:
- single page parse (fetch_and_extract)
- deep crawl with per-domain page limit and time budget
"""

from __future__ import annotations

from typing import Set, Tuple
from urllib.parse import urlparse

from crawler.web_crawler import Crawler

from emailbot import settings
from emailbot.web_extract import fetch_and_extract


async def crawl_emails(start_url: str, limit_pages: int | None = None) -> Tuple[str, Set[str]]:
    """Crawl ``start_url`` deeply returning normalized URL and found e-mails."""

    limit = limit_pages if limit_pages is not None else settings.CRAWL_MAX_PAGES_PER_DOMAIN
    max_pages = limit if limit and limit > 0 else None

    seen: Set[str] = set()
    final_start, on_start = await fetch_and_extract(start_url)
    seen |= set(on_start or [])

    crawler = Crawler(final_start, max_pages=max_pages)
    domain = urlparse(final_start).netloc
    processed = 1 if limit and limit > 0 else 0

    try:
        async for url, _html in crawler.crawl():
            if limit and processed >= limit:
                break
            if not url:
                continue
            if urlparse(url).netloc != domain:
                continue
            if url == final_start:
                continue
            try:
                _, emails = await fetch_and_extract(url)
            except Exception:
                continue
            seen |= set(emails or [])
            if processed:
                processed += 1
            if crawler.stopped:
                break
    finally:
        await crawler.close()

    return final_start, seen

