"""Asynchronous web crawler used for deep URL extraction."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import AsyncIterator, Callable, Optional, Sequence
from urllib import robotparser
from urllib.parse import urlparse

import httpx

try:  # pragma: no cover - optional dependency
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - fallback parser
    BeautifulSoup = None
    from html.parser import HTMLParser

    class _LinkExtractor(HTMLParser):
        def __init__(self) -> None:
            super().__init__()
            self.links: list[str] = []

        def handle_starttag(self, tag: str, attrs) -> None:  # type: ignore[override]
            if tag.lower() != "a":
                return
            for name, value in attrs:
                if name.lower() == "href" and value:
                    self.links.append(value)
                    break

    def _fallback_links(html: str) -> list[str]:
        parser = _LinkExtractor()
        try:
            parser.feed(html)
        except Exception:
            return []
        return parser.links

from emailbot import config as C
from utils.charset_helper import best_effort_decode
from utils.url_tools import canonicalize, same_domain


class Crawler:
    """Simple breadth-first crawler with robots.txt support."""

    def __init__(
        self,
        start_url: str,
        *,
        max_pages: int | None = None,
        max_depth: int | None = None,
        on_page: Optional[Callable[[int, str], None]] = None,
        path_prefixes: Sequence[str] | None = None,
    ) -> None:
        self.start = start_url
        self.start_canonical = canonicalize(start_url, start_url) or start_url
        self.max_pages = max_pages or C.CRAWL_MAX_PAGES
        self.max_depth = max_depth or C.CRAWL_MAX_DEPTH
        self.client = httpx.AsyncClient(
            http2=True,
            timeout=20,
            headers={"User-Agent": C.CRAWL_USER_AGENT},
            follow_redirects=True,
        )
        self.robots = robotparser.RobotFileParser()
        self.pages_scanned = 0
        self.on_page = on_page
        self.allowed_prefixes = self._normalize_prefixes(path_prefixes)
        try:
            robots_url = canonicalize(start_url, "/robots.txt") or start_url
            self.robots.set_url(robots_url)
            self.robots.read()
        except Exception:
            pass

    @staticmethod
    def _normalize_prefixes(prefixes: Sequence[str] | None) -> list[str]:
        result: list[str] = []
        if not prefixes:
            return result
        for raw in prefixes:
            if not isinstance(raw, str):
                continue
            cleaned = raw.strip()
            if not cleaned:
                continue
            if not cleaned.startswith("/"):
                cleaned = "/" + cleaned
            if cleaned not in result:
                result.append(cleaned)
        return result

    def _path_allowed(self, url: str) -> bool:
        if not self.allowed_prefixes:
            return True
        try:
            path = urlparse(url).path or "/"
        except Exception:
            return False
        return any(path.startswith(prefix) for prefix in self.allowed_prefixes)

    async def close(self) -> None:
        """Close the underlying HTTP client."""

        try:
            await self.client.aclose()
        except Exception:
            pass

    def allowed(self, url: str) -> bool:
        """Return ``True`` if ``url`` is allowed by robots.txt."""

        try:
            return self.robots.can_fetch(C.CRAWL_USER_AGENT, url)
        except Exception:
            return True

    async def fetch_html(self, url: str) -> tuple[str | None, str | None]:
        """Fetch ``url`` and return ``(final_url, html)`` if it looks like HTML."""

        try:
            response = await self.client.get(url)
            content_type = str(response.headers.get("content-type", "")).lower()
            if content_type and "html" not in content_type and "text" not in content_type:
                return str(response.url), None
            text = best_effort_decode(response.content)
            if not text:
                response.encoding = response.encoding or "utf-8"
                text = response.text
            return str(response.url), text
        except Exception:
            return None, None

    def extract_links(self, base: str, html: str) -> list[str]:
        """Extract and canonicalize links from ``html``."""
        raw_links: list[str]
        if BeautifulSoup is not None:
            soup = BeautifulSoup(html, "html.parser")
            raw_links = [anchor.get("href", "") for anchor in soup.find_all("a", href=True)]
        else:  # pragma: no cover - fallback without bs4
            raw_links = _fallback_links(html)
        result: list[str] = []
        for href in raw_links:
            candidate = canonicalize(base, href)
            if candidate:
                result.append(candidate)
        return result

    async def crawl(self) -> AsyncIterator[tuple[str, str]]:
        """Iterate over fetched pages yielding ``(url, html)`` pairs."""

        queue: deque[tuple[str, int]] = deque()
        start_url = self.start_canonical
        queue.append((start_url, 0))
        seen: set[str] = set()
        queued: set[str] = {start_url}
        while queue and self.pages_scanned < self.max_pages:
            url, depth = queue.popleft()
            if url in seen:
                continue
            seen.add(url)
            if C.CRAWL_SAME_DOMAIN and not same_domain(self.start, url):
                continue
            if not self.allowed(url):
                continue
            if C.CRAWL_DELAY_SEC:
                try:
                    await asyncio.sleep(C.CRAWL_DELAY_SEC)
                except Exception:
                    pass
            final_url, html = await self.fetch_html(url)
            if not html:
                continue
            target_url = final_url or url
            seen.add(target_url)
            include_page = self._path_allowed(target_url)
            if include_page:
                self.pages_scanned += 1
                if self.on_page:
                    try:
                        self.on_page(self.pages_scanned, target_url)
                    except Exception:
                        pass
                yield target_url, html
            if depth >= self.max_depth:
                continue
            for link in self.extract_links(target_url, html):
                if link in seen or link in queued:
                    continue
                if C.CRAWL_SAME_DOMAIN and not same_domain(self.start, link):
                    continue
                if not self._path_allowed(link):
                    continue
                queue.append((link, depth + 1))
                queued.add(link)

