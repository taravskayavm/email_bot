"""Helpers for fetching pages and extracting e-mail addresses from HTML."""

from __future__ import annotations

import logging
import re
from typing import Set, Tuple

import httpx
from bs4 import BeautifulSoup
from charset_normalizer import from_bytes

from . import settings
from .sanitizer import ZWSP_CHARS, dedup_emails, normalize_email

logger = logging.getLogger(__name__)

MAIL_RE = re.compile(
    r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+",
    re.UNICODE,
)
SOFT_HYPHEN = "\u00ad"
_REMOVE_MAP = {ord(ch): None for ch in [*ZWSP_CHARS, SOFT_HYPHEN]}


def _clean(text: str) -> str:
    """Normalise extracted text removing zero-width junk and fixing separators."""

    if not text:
        return ""
    cleaned = text.translate(_REMOVE_MAP)
    cleaned = cleaned.replace("\u00a0", " ")
    cleaned = re.sub(r"\s*[\[\(\{]\s*at\s*[\]\)\}]\s*", "@", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*@\s*", "@", cleaned)
    return cleaned.strip()


def extract_emails_from_html(html: str | None) -> Set[str]:
    """Return a set of e-mail addresses found in ``html``."""

    if not html:
        return set()

    soup = BeautifulSoup(html, "html.parser")

    candidates: list[str] = []

    for tag in soup.find_all("a"):
        href = tag.get("href")
        if not href or not href.lower().startswith("mailto:"):
            continue
        candidate = href.split(":", 1)[1]
        if "?" in candidate:
            candidate = candidate.split("?", 1)[0]
        candidate = _clean(candidate)
        norm = normalize_email(candidate)
        if norm:
            candidates.append(norm)

    text_block = _clean(soup.get_text(" "))
    for match in MAIL_RE.finditer(text_block):
        candidate = _clean(match.group(0))
        norm = normalize_email(candidate)
        if norm:
            candidates.append(norm)

    unique = dedup_emails(candidates)
    return set(unique)


async def fetch_and_extract(
    url: str, *, timeout: int | None = None, user_agent: str | None = None
) -> Tuple[str, Set[str]]:
    """Fetch ``url`` and return ``(final_url, {emails})``."""

    ua = (
        user_agent
        or getattr(settings, "CRAWL_USER_AGENT", None)
        or getattr(settings, "WEB_USER_AGENT", None)
        or "Mozilla/5.0 EmailBotCrawler/1.0"
    )
    timeout_value = timeout or int(getattr(settings, "CRAWL_GET_TIMEOUT", 20))
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru,en;q=0.8",
        "Cache-Control": "no-cache",
    }
    http2_enabled = getattr(settings, "CRAWL_HTTP2", None)
    if isinstance(http2_enabled, bool):
        http2 = http2_enabled
    else:
        http2 = bool(int(http2_enabled)) if http2_enabled is not None else True

    final_url = url
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=timeout_value,
        http2=http2,
        headers=headers,
    ) as client:
        response = await client.get(url)
        response.raise_for_status()
        final_url = str(response.url)
        content = response.content or b""

    max_bytes = getattr(settings, "WEB_MAX_BYTES", 0) or 0
    if max_bytes and len(content) > max_bytes:
        content = content[:max_bytes]

    html: str | None = None
    try:
        detection = from_bytes(content).best()
        if detection is not None:
            html = str(detection)
    except Exception:
        pass

    if html is None:
        encoding = getattr(response, "encoding", None) or "utf-8"
        try:
            html = content.decode(encoding, errors="replace")
        except Exception:
            html = content.decode("cp1251", errors="replace")

    return final_url, extract_emails_from_html(html)


__all__ = ["MAIL_RE", "ZWSP_CHARS", "extract_emails_from_html", "fetch_and_extract"]
