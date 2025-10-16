"""Helpers for fetching pages and extracting e-mail addresses from HTML."""

from __future__ import annotations

import logging
import re
from typing import Iterable, Optional, Set, Tuple

import httpx
from bs4 import BeautifulSoup

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


def _decode_content(content: bytes, encoding: Optional[str]) -> str:
    candidates: Iterable[Optional[str]] = (encoding, "cp1251", "utf-8")
    for candidate in candidates:
        if not candidate:
            continue
        try:
            return content.decode(candidate, errors="ignore")
        except Exception:
            continue
    return content.decode("utf-8", errors="ignore")


def fetch_and_extract(url: str) -> Tuple[str, Set[str]]:
    """Fetch ``url`` and return ``(final_url, {emails})``."""

    headers = {
        "User-Agent": settings.WEB_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru,en;q=0.9",
        "Cache-Control": "no-cache",
    }
    final_url = url
    candidates: list[str] = []
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=settings.WEB_FETCH_TIMEOUT,
            headers=headers,
            http2=getattr(settings, "WEB_HTTP2", True),
        ) as client:
            response = client.get(url)
            final_url = str(response.url)
            data = response.content
    except Exception as exc:  # pragma: no cover - defensive
        # не валим поток, просто логируем причину
        logger.info("web extract failed for %s: %s", url, exc)
        return final_url, set()

    max_bytes = getattr(settings, "WEB_MAX_BYTES", 0) or 0
    if max_bytes and len(data) > max_bytes:
        data = data[:max_bytes]

    text = _decode_content(data, getattr(response, "encoding", None))
    soup = BeautifulSoup(text, "html.parser")

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
    return final_url, set(unique)


__all__ = ["MAIL_RE", "ZWSP_CHARS", "fetch_and_extract"]
