"""Asynchronous web crawler used for deep URL extraction."""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from pathlib import Path
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


class RobotsCache:
    """Simple persistent cache for robots.txt contents."""

    def __init__(self, cache_path: str, ttl_seconds: int) -> None:
        self.cache_path = Path(cache_path)
        self.ttl_seconds = ttl_seconds
        self._data: dict[str, dict[str, object]] | None = None

    def _ensure_loaded(self) -> None:
        if self._data is not None:
            return
        try:
            raw = self.cache_path.read_text(encoding="utf-8")
            data = json.loads(raw)
            if isinstance(data, dict):
                self._data = data
                return
        except Exception:
            pass
        self._data = {}

    def _save(self) -> None:
        if self._data is None:
            return
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        try:
            self.cache_path.write_text(json.dumps(self._data, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def get(self, host: str, now: float) -> str | None:
        self._ensure_loaded()
        if not self._data:
            return None
        entry = self._data.get(host)
        if not isinstance(entry, dict):
            return None
        ts = entry.get("ts")
        if not isinstance(ts, (int, float)):
            return None
        if now - float(ts) > self.ttl_seconds:
            return None
        text = entry.get("text")
        if isinstance(text, str):
            return text
        return None

    def set(self, host: str, text: str, now: float) -> None:
        self._ensure_loaded()
        assert self._data is not None  # for mypy
        self._data[host] = {"ts": int(now), "text": str(text or "")}
        self._save()


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
        timeout = httpx.Timeout(connect=10.0, read=20.0, write=20.0, pool=10.0)
        limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
        self.client = httpx.AsyncClient(
            http2=C.CRAWL_HTTP2,
            timeout=timeout,
            headers={"User-Agent": C.CRAWL_USER_AGENT},
            follow_redirects=True,
            limits=limits,
        )
        self.pages_scanned = 0
        self.on_page = on_page
        self.allowed_prefixes = self._normalize_prefixes(path_prefixes)
        self.last_error: Exception | None = None
        self._start_ts = time.monotonic()
        self._domain_pages: dict[str, int] = {}
        self._robots_cache = RobotsCache(C.ROBOTS_CACHE_PATH, C.ROBOTS_CACHE_TTL_SECONDS)

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

    async def allowed(self, url: str) -> bool:
        """Return ``True`` if ``url`` is allowed by robots.txt."""

        parser = await self._get_robots_parser(url)
        if parser is None:
            return True
        try:
            return parser.can_fetch(C.CRAWL_USER_AGENT, url)
        except Exception:
            return True

    async def fetch_html(self, url: str) -> tuple[str | None, str | None]:
        """Fetch ``url`` and return ``(final_url, html)`` if it looks like HTML."""

        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = await self.client.get(url)
                content_type = str(response.headers.get("content-type", "")).lower()
                if (
                    content_type
                    and "html" not in content_type
                    and "text" not in content_type
                ):
                    self.last_error = None
                    return str(response.url), None
                text = best_effort_decode(response.content)
                if not text:
                    response.encoding = response.encoding or "utf-8"
                    text = response.text
                self.last_error = None
                return str(response.url), text
            except httpx.ReadTimeout as exc:
                last_error = exc
                self.last_error = exc
            except Exception as exc:  # pragma: no cover - defensive network errors
                last_error = exc
                self.last_error = exc
            if attempt < 2:
                try:
                    await asyncio.sleep(0.5 * (attempt + 1))
                except Exception:
                    pass
                continue
            break
        if last_error is not None:
            self.last_error = last_error
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
            if self._time_budget_exceeded():
                break
            url, depth = queue.popleft()
            if url in seen:
                continue
            seen.add(url)
            if C.CRAWL_SAME_DOMAIN and not same_domain(self.start, url):
                continue
            if not self._domain_budget_allows(url):
                continue
            if not await self.allowed(url):
                continue
            self._mark_domain_usage(url)
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

    def _time_budget_exceeded(self) -> bool:
        if C.CRAWL_TIME_BUDGET_SECONDS <= 0:
            return False
        return (time.monotonic() - self._start_ts) > C.CRAWL_TIME_BUDGET_SECONDS

    def _domain_budget_allows(self, url: str) -> bool:
        if C.CRAWL_MAX_PAGES_PER_DOMAIN <= 0:
            return True
        host = urlparse(url).netloc
        if not host:
            return True
        count = self._domain_pages.get(host, 0)
        return count < C.CRAWL_MAX_PAGES_PER_DOMAIN

    def _mark_domain_usage(self, url: str) -> None:
        host = urlparse(url).netloc
        if not host:
            return
        self._domain_pages[host] = self._domain_pages.get(host, 0) + 1

    async def _get_robots_parser(self, url: str) -> robotparser.RobotFileParser | None:
        parsed = urlparse(url)
        host = parsed.netloc
        if not host:
            return None
        scheme = parsed.scheme or "https"
        robots_url = f"{scheme}://{host}/robots.txt"
        now = time.time()
        cached_text = self._robots_cache.get(host, now)
        if cached_text is None:
            text = await self._download_robots(robots_url)
            cached_text = text if text is not None else ""
            self._robots_cache.set(host, cached_text, now)
        parser = robotparser.RobotFileParser()
        parser.set_url(robots_url)
        try:
            parser.parse(cached_text.splitlines())
        except Exception:
            return None
        return parser

    async def _download_robots(self, robots_url: str) -> str | None:
        try:
            response = await self.client.get(robots_url)
        except Exception:
            return None
        if response.status_code != 200:
            return ""
        try:
            return best_effort_decode(response.content) or response.text
        except Exception:
            return ""

