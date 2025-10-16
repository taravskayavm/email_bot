"""Utilities for fetching web pages and extracting e-mail addresses."""

from __future__ import annotations

import os
import re
from typing import List, Set, Tuple

import httpx
from bs4 import BeautifulSoup
from bs4 import FeatureNotFound

# переиспользуем уже существующий шаблон regex, если он есть в проекте
EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9.-]+")

_UA = os.getenv("WEB_USER_AGENT", "Mozilla/5.0")
_TIMEOUT = float(os.getenv("WEB_FETCH_TIMEOUT", "30"))
_MAX_BYTES = int(os.getenv("WEB_MAX_BYTES", "3000000"))
_ALLOW_REDIRECTS = True


async def _fetch_html(url: str) -> Tuple[str, str]:
    """Fetch ``url`` and return a tuple of ``(final_url, html_text)``."""

    limits = httpx.Limits(max_connections=3, max_keepalive_connections=3)
    async with httpx.AsyncClient(
        http2=True,
        headers={"User-Agent": _UA},
        limits=limits,
        follow_redirects=_ALLOW_REDIRECTS,
        timeout=_TIMEOUT,
    ) as cli:
        response = await cli.get(url)
        response.raise_for_status()
        content = response.content[:_MAX_BYTES]
        text = content.decode(response.encoding or "utf-8", errors="replace")
        return str(response.url), text


def _extract_emails_from_html(html: str) -> Set[str]:
    """Return a set of e-mail addresses extracted from ``html`` text."""

    try:
        soup = BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    found = set(match.group(0) for match in EMAIL_RE.finditer(text))
    return {candidate.strip().strip(".,;:()[]{}<>") for candidate in found if "@" in candidate}


async def extract_from_url(url: str, context=None) -> List[str]:
    """Fetch a web page and return sorted unique e-mail addresses."""

    final_url, html = await _fetch_html(url)
    _ = final_url  # unused placeholder for potential future logic
    emails = _extract_emails_from_html(html)
    return sorted(emails)


__all__ = ["extract_from_url"]
