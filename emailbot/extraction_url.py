"""Helpers for extracting e-mail hits from URLs."""

from __future__ import annotations

import re
import time
import urllib.parse
import urllib.request
from typing import Dict, List, Optional, Tuple

from .extraction import EmailHit, _valid_local, _valid_domain
from .extraction_common import normalize_text
from . import settings

_OBFUSCATED_RE = re.compile(
    r"(?P<local>[\w.+-]+)\s*(?P<at>@|\(at\)|\[at\]|at|собака)\s*(?P<domain>[\w-]+(?:\s*(?:\.|dot|\(dot\)|\[dot\]|точка)\s*[\w-]+)+)",
    re.I,
)

_DOT_SPLIT_RE = re.compile(r"\s*(?:\.|dot|\(dot\)|\[dot\]|точка)\s*", re.I)

_CACHE: Dict[str, Tuple[float, str]] = {}
_READ_CHUNK = 128 * 1024


def decode_cfemail(hexstr: str) -> str:
    """Decode Cloudflare email obfuscation string."""

    key = int(hexstr[:2], 16)
    decoded = bytes(int(hexstr[i : i + 2], 16) ^ key for i in range(2, len(hexstr), 2))
    return decoded.decode("utf-8", "ignore")


def fetch_url(
    url: str,
    stop_event: Optional[object] = None,
    *,
    ttl: int = 300,
    timeout: int = 15,
    max_size: int = 1_000_000,
    allowed_schemes: Tuple[str, ...] = ("http", "https"),
    allowed_tlds: Optional[set[str]] = None,
) -> Optional[str]:
    """Fetch ``url`` and return decoded text respecting several limits."""

    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in allowed_schemes:
        return None
    if allowed_tlds and parsed.hostname:
        tld = parsed.hostname.rsplit(".", 1)[-1].lower()
        if tld not in allowed_tlds:
            return None
    now = time.time()
    cached = _CACHE.get(url)
    if cached and cached[0] > now:
        return cached[1]
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            final_url = resp.geturl()
            final_parsed = urllib.parse.urlparse(final_url)
            if final_parsed.scheme not in allowed_schemes:
                return None
            if parsed.netloc and final_parsed.netloc and parsed.netloc != final_parsed.netloc:
                return None
            if allowed_tlds and final_parsed.hostname:
                tld = final_parsed.hostname.rsplit(".", 1)[-1].lower()
                if tld not in allowed_tlds:
                    return None
            encoding = resp.headers.get_content_charset() or "utf-8"
            chunks: List[bytes] = []
            total = 0
            while True:
                if stop_event and getattr(stop_event, "is_set", lambda: False)():
                    return None
                chunk = resp.read(_READ_CHUNK)
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total >= max_size:
                    break
            data = b"".join(chunks)
            text = data.decode(encoding, "ignore")
    except Exception:  # pragma: no cover - network errors
        return None
    _CACHE[url] = (now + ttl, text)
    return text


def extract_obfuscated_hits(
    text: str, source_ref: str, stats: Optional[Dict[str, int]] = None
) -> List[EmailHit]:
    """Return all ``EmailHit`` objects found via obfuscation patterns."""

    settings.load()
    text = normalize_text(text)
    hits: List[EmailHit] = []
    for m in _OBFUSCATED_RE.finditer(text):
        local = m.group("local")
        at_token = m.group("at")
        domain_raw = m.group("domain")
        parts = [p for p in _DOT_SPLIT_RE.split(domain_raw) if p]
        domain = ".".join(parts)
        email = f"{local}@{domain}".lower()
        if not (_valid_local(local) and _valid_domain(domain)):
            continue
        if settings.STRICT_OBFUSCATION and local.isdigit():
            if stats is not None:
                stats["numeric_from_obfuscation_dropped"] = stats.get(
                    "numeric_from_obfuscation_dropped", 0
                ) + 1
            continue
        start, end = m.span()
        pre = text[max(0, start - 16) : start]
        post = text[end : end + 16]
        hits.append(
            EmailHit(email=email, source_ref=source_ref, origin="obfuscation", pre=pre, post=post)
        )
    return hits


__all__ = ["extract_obfuscated_hits", "fetch_url", "decode_cfemail"]

