"""Heuristic detection of popular top-level site sections."""

from __future__ import annotations

from collections import Counter
import re
import urllib.parse
from typing import Iterable

import httpx

_HREF_RX = re.compile(r"""<a\s+[^>]*href=["']([^"'#]+)["']""", re.IGNORECASE)


def _absolute_url(base: str, href: str) -> str:
    """Return ``href`` resolved against ``base`` ignoring errors."""

    try:
        return urllib.parse.urljoin(base, href)
    except Exception:
        return href


def _same_host(base: str, url: str) -> bool:
    """Check whether ``url`` points to the same host as ``base``."""

    try:
        base_parts = urllib.parse.urlsplit(base)
        url_parts = urllib.parse.urlsplit(url)
    except Exception:
        return False
    if url_parts.scheme not in {"http", "https"}:
        return False
    return (url_parts.netloc or "").lower() == (base_parts.netloc or "").lower()


def _top_prefix(path: str) -> str:
    """Return the top-most path segment prefixed with ``/``."""

    clean = (path or "/").split("?", 1)[0]
    if not clean.startswith("/"):
        clean = "/" + clean
    parts = [segment for segment in clean.split("/") if segment]
    if not parts:
        return "/"
    return "/" + parts[0]


def _score_ok(prefix: str) -> bool:
    """Return ``True`` when ``prefix`` looks like a useful section."""

    bad: set[str] = {"", "/", "/#", "/static", "/assets", "/img", "/images", "/css", "/js", "/api"}
    if prefix in bad:
        return False
    if any(prefix.startswith(p) for p in ("/static", "/assets", "/media", "/wp-", "/bitrix", "/upload")):
        return False
    if len(prefix) <= 2:
        return False
    return True


def _count_prefixes(hrefs: Iterable[str], base_url: str) -> Counter[str]:
    """Count occurrences of valid top-level prefixes for ``base_url``."""

    counts: Counter[str] = Counter()
    for href in hrefs:
        absolute = _absolute_url(base_url, href)
        if not _same_host(base_url, absolute):
            continue
        try:
            path = urllib.parse.urlsplit(absolute).path or "/"
        except Exception:
            continue
        prefix = _top_prefix(path)
        if _score_ok(prefix):
            counts[prefix] += 1
    return counts


def discover_sections(base_url: str, *, max_candidates: int = 12, timeout: float = 10.0) -> list[str]:
    """Discover likely section prefixes for ``base_url``.

    The function fetches ``base_url`` and analyses anchor links, returning up to
    ``max_candidates`` prefixes such as ``/news`` or ``/catalog``. Only links on
    the same host are considered.
    """

    cleaned = (base_url or "").strip()
    if not cleaned:
        return []
    try:
        response = httpx.get(
            cleaned,
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "emailbot/sections"},
        )
        html = response.text or ""
    except Exception:
        return []

    hrefs = _HREF_RX.findall(html)
    counts = _count_prefixes(hrefs, cleaned)
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [prefix for prefix, _ in ordered[: max(0, max_candidates)]]

