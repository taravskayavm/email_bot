"""Helpers to fetch and extract e-mail addresses from a single URL."""

from __future__ import annotations

import re
from typing import Dict, List, Tuple

import aiohttp

from emailbot.utils.email_clean import clean_and_normalize_email

URL_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}")


class PageFetchError(Exception):
    """Raised when a page cannot be downloaded."""


async def fetch_html(url: str, *, total_timeout: int = 15) -> str:
    headers = {
        "User-Agent": "EmailBot/1.0 (+bot contact via reply)",
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
    }
    timeout = aiohttp.ClientTimeout(total=total_timeout, connect=10, sock_read=10)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        async with session.get(url, allow_redirects=True) as response:
            if response.status != 200:
                raise PageFetchError(f"http_status_{response.status}")
            text = await response.text()
            if len(text) > 3_000_000:
                raise PageFetchError("page_too_large")
            return text


def extract_from_html(html: str) -> Tuple[List[str], Dict[str, int]]:
    candidates = URL_EMAIL_RE.findall(html or "")
    ok: List[str] = []
    rejects: Dict[str, int] = {}
    seen: set[str] = set()
    for raw in candidates:
        email, reason = clean_and_normalize_email(raw)
        if email is None:
            key = str(reason) if reason else "unknown"
            rejects[key] = rejects.get(key, 0) + 1
            continue
        if email in seen:
            continue
        seen.add(email)
        ok.append(email)
    return ok, rejects


async def ingest_url_once(url: str) -> Tuple[List[str], Dict[str, int]]:
    html = await fetch_html(url)
    return extract_from_html(html)
