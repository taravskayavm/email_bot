"""Asynchronous web crawler used for deep URL extraction."""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Callable, Optional, Sequence
from urllib import robotparser
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

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
from emailbot.run_control import should_stop
from emailbot.settings import (
    ALLOWED_CONTENT_TYPES,
    ENABLE_SITEMAP,
    GET_TIMEOUT,
    HEAD_TIMEOUT,
    MAX_CONTENT_LENGTH,
    SITEMAP_MAX_URLS,
)
from utils.charset_helper import best_effort_decode
from utils.url_tools import canonicalize, same_domain


BACKOFF_BASE_SECS = 1
BACKOFF_MAX_SECS = 60
BACKOFF_STATUS = {429, 503}
BACKOFF_TTL_SECS = 60

LEGACY_MODE = os.getenv("LEGACY_MODE", "0") == "1"


@dataclass
class _BackoffState:
    fail_count: int = 0
    until_ts: float = 0.0


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
        stop_cb: Optional[Callable[[], bool]] = None,
    ) -> None:
        self.start = start_url
        self.start_canonical = canonicalize(start_url, start_url) or start_url
        default_max_pages = C.CRAWL_MAX_PAGES
        default_max_depth = C.CRAWL_MAX_DEPTH
        if LEGACY_MODE:
            default_max_depth = 1
        self.max_pages = max_pages if max_pages is not None else default_max_pages
        self.max_depth = max_depth if max_depth is not None else default_max_depth
        timeout = httpx.Timeout(
            connect=HEAD_TIMEOUT,
            read=GET_TIMEOUT,
            write=GET_TIMEOUT,
            pool=HEAD_TIMEOUT,
        )
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
        self._head_checks: dict[str, bool] = {}
        self._allowed_types = {item.strip().lower() for item in ALLOWED_CONTENT_TYPES if item.strip()}
        self._host_backoff: dict[str, _BackoffState] = {}
        self.stop_cb = stop_cb
        self.stopped = False
        budget = C.CRAWL_TIME_BUDGET_SECONDS
        if LEGACY_MODE:
            if budget <= 0:
                budget = 45
            else:
                budget = min(budget, 45)
        self._time_budget_limit = budget
        self._frontier_cap = 150 if LEGACY_MODE else 0

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

    def _stop_requested(self) -> bool:
        if should_stop():
            return True
        if self.stop_cb is None:
            return False
        try:
            return bool(self.stop_cb())
        except Exception:
            return should_stop()

    def _host_backoff_sleep(self, host: str) -> float:
        state = self._host_backoff.get(host)
        if not state:
            return 0.0
        now = time.time()
        if state.until_ts > now:
            return max(0.0, state.until_ts - now)
        return 0.0

    def _host_register_fail(self, host: str) -> None:
        if not host:
            return
        state = self._host_backoff.setdefault(host, _BackoffState())
        state.fail_count += 1
        delay = min(BACKOFF_BASE_SECS * (2 ** (state.fail_count - 1)), BACKOFF_MAX_SECS)
        state.until_ts = time.time() + max(delay, BACKOFF_TTL_SECS)

    def _host_register_ok(self, host: str) -> None:
        if not host:
            return
        self._host_backoff.pop(host, None)

    async def _request_with_backoff(self, method: str, url: str, **kwargs) -> httpx.Response | None:
        if self._stop_requested():
            self.stopped = True
            return None
        host = urlparse(url).netloc
        if host:
            sleep_for = self._host_backoff_sleep(host)
            if sleep_for > 0:
                if self._stop_requested():
                    self.stopped = True
                    return None
                try:
                    await asyncio.sleep(sleep_for)
                except Exception:
                    pass
        try:
            response = await self.client.request(method, url, **kwargs)
            if self._stop_requested():
                self.stopped = True
                return None
        except httpx.HTTPError:
            if host:
                self._host_register_fail(host)
            raise
        if host:
            if response.status_code in BACKOFF_STATUS:
                self._host_register_fail(host)
                return None
            self._host_register_ok(host)
        return response

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
            if self._stop_requested():
                self.stopped = True
                return None, None
            try:
                response = await self._request_with_backoff("GET", url)
                if response is None:
                    last_error = None
                    self.last_error = None
                    if attempt < 2:
                        try:
                            await asyncio.sleep(0.5 * (attempt + 1))
                        except Exception:
                            pass
                        continue
                    break
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
                if self._stop_requested():
                    self.stopped = True
                    return None, None
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
        frontier_cap = self._frontier_cap
        if ENABLE_SITEMAP:
            try:
                sitemap_urls = await self._load_sitemap_urls(self.start)
            except Exception:
                sitemap_urls = []
            for extra in sitemap_urls:
                if extra in queued:
                    continue
                queue.append((extra, 0))
                queued.add(extra)
        while queue and self.pages_scanned < self.max_pages:
            if self._stop_requested():
                self.stopped = True
                break
            if self._time_budget_exceeded():
                break
            url, depth = queue.popleft()
            if self._stop_requested():
                self.stopped = True
                break
            if url in seen:
                continue
            seen.add(url)
            if C.CRAWL_SAME_DOMAIN and not same_domain(self.start, url):
                continue
            if not self._domain_budget_allows(url):
                continue
            if not await self.allowed(url):
                continue
            if not await self._passes_head_filter(url):
                continue
            self._mark_domain_usage(url)
            if C.CRAWL_DELAY_SEC:
                if self._stop_requested():
                    self.stopped = True
                    break
                try:
                    await asyncio.sleep(C.CRAWL_DELAY_SEC)
                except Exception:
                    pass
            if self._stop_requested():
                self.stopped = True
                break
            final_url, html = await self.fetch_html(url)
            if self.stopped:
                break
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
            added_links = 0
            for link in self.extract_links(target_url, html):
                if self._stop_requested():
                    self.stopped = True
                    break
                if frontier_cap and (added_links >= frontier_cap or len(queue) >= frontier_cap):
                    break
                if link in seen or link in queued:
                    continue
                if C.CRAWL_SAME_DOMAIN and not same_domain(self.start, link):
                    continue
                if not self._path_allowed(link):
                    continue
                queue.append((link, depth + 1))
                queued.add(link)
                if frontier_cap:
                    added_links += 1
            if self.stopped:
                break

    def _time_budget_exceeded(self) -> bool:
        if self._time_budget_limit <= 0:
            return False
        return (time.monotonic() - self._start_ts) > self._time_budget_limit

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
            response = await self._request_with_backoff("GET", robots_url)
        except Exception:
            return None
        if response is None:
            return None
        if response.status_code != 200:
            return ""
        try:
            return best_effort_decode(response.content) or response.text
        except Exception:
            return ""

    async def _passes_head_filter(self, url: str) -> bool:
        if LEGACY_MODE:
            self._head_checks[url] = True
            return True
        cached = self._head_checks.get(url)
        if cached is not None:
            return cached

        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            self._head_checks[url] = True
            return True

        allowed = True
        try:
            response = await self._request_with_backoff("HEAD", url, timeout=HEAD_TIMEOUT)
            if response is None:
                allowed = False
            else:
                status = response.status_code
                if status >= 400 and status not in {405, 501}:
                    allowed = False
                else:
                    content_type = (response.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
                    length_header = response.headers.get("content-length")
                    content_length = 0
                    if length_header:
                        try:
                            content_length = int(length_header)
                        except (TypeError, ValueError):
                            content_length = 0
                    if self._allowed_types and content_type and content_type not in self._allowed_types:
                        allowed = False
                    if MAX_CONTENT_LENGTH > 0 and content_length > MAX_CONTENT_LENGTH:
                        allowed = False
                    if status in {405, 501}:
                        allowed = True
        except httpx.TimeoutException:
            allowed = False
        except httpx.HTTPStatusError:
            allowed = False
        except httpx.RequestError:
            allowed = True
        except Exception:
            allowed = True

        self._head_checks[url] = allowed
        return allowed

    async def _load_sitemap_urls(self, seed_url: str) -> list[str]:
        if LEGACY_MODE:
            return []
        if not ENABLE_SITEMAP or SITEMAP_MAX_URLS <= 0:
            return []

        parsed = urlparse(seed_url)
        if not parsed.scheme or not parsed.netloc:
            return []

        base = f"{parsed.scheme}://{parsed.netloc}"
        candidates = ["/sitemap.xml", "/sitemap_index.xml"]
        urls: list[str] = []
        seen: set[str] = set()

        for suffix in candidates:
            sitemap_url = urljoin(base.rstrip("/") + "/", suffix.lstrip("/"))
            if self._stop_requested():
                self.stopped = True
                break
            try:
                response = await self._request_with_backoff("GET", sitemap_url, timeout=GET_TIMEOUT)
            except Exception:
                continue
            if response is None:
                continue
            if response.status_code != 200:
                continue
            if self._stop_requested():
                self.stopped = True
                break
            try:
                text = response.text
            except Exception:
                try:
                    text = best_effort_decode(response.content)
                except Exception:
                    text = ""
            if not text:
                continue
            try:
                root = ET.fromstring(text)
            except Exception:
                continue
            for node in root.iter():
                if self._stop_requested():
                    self.stopped = True
                    break
                tag = node.tag.lower()
                if not tag.endswith("loc"):
                    continue
                value = (node.text or "").strip()
                if not value:
                    continue
                candidate = canonicalize(base, value) or value
                if not candidate:
                    continue
                if not same_domain(self.start, candidate):
                    continue
                if candidate in seen:
                    continue
                seen.add(candidate)
                urls.append(candidate)
                if len(urls) >= SITEMAP_MAX_URLS:
                    return urls
            if self.stopped:
                break
        if self.stopped:
            return urls
        return urls

