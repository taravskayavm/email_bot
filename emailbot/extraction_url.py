"""Helpers for extracting e-mail hits from URLs."""

from __future__ import annotations

import re
import time
import urllib.parse
from typing import Dict, List, Optional, Tuple, Callable, Protocol
import json
import os
import httpx
import urllib.request

from .extraction import EmailHit, _valid_local, _valid_domain, extract_emails_document
from .extraction_common import normalize_text, maybe_decode_base64, strip_phone_prefix
from emailbot import settings
from emailbot.settings_store import get

_OBFUSCATED_RE = re.compile(
    r"(?P<local>[\w.+-]+)\s*(?P<at>@|\(at\)|\[at\]|{at}|at|собака|arroba)\s*(?P<domain>[\w-]+(?:\s*(?:\.|dot|\(dot\)|\[dot\]|{dot}|точка|ponto)\s*[\w-]+)+)",
    re.I,
)

_DOT_SPLIT_RE = re.compile(r"\s*(?:\.|dot|\(dot\)|\[dot\]|{dot}|точка|ponto)\s*", re.I)

_CACHE: Dict[str, Tuple[float, str]] = {}
_CACHE_BYTES: Dict[str, Tuple[float, bytes]] = {}
_CURRENT_BATCH: str | None = None
_READ_CHUNK = 128 * 1024
_SIMPLE_EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._%+\-])[A-Za-z0-9._%+\-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
)


def _fetch_get(url: str):
    return httpx.get(url, timeout=15)


def _fetch_stream(method: str, url: str):
    return httpx.stream(method, url, timeout=15)


class ResponseLike(Protocol):
    """Minimal protocol for mocked HTTP responses."""

    text: str
    content: bytes


def decode_cfemail(hexstr: str) -> str:
    """Decode Cloudflare email obfuscation string."""

    key = int(hexstr[:2], 16)
    decoded = bytes(int(hexstr[i : i + 2], 16) ^ key for i in range(2, len(hexstr), 2))
    return decoded.decode("utf-8", "ignore")


def set_batch(batch_id: str | None) -> None:
    """Switch caches when a new extraction batch starts."""

    global _CURRENT_BATCH
    if batch_id != _CURRENT_BATCH:
        _CACHE.clear()
        _CACHE_BYTES.clear()
        _CURRENT_BATCH = batch_id


def fetch_url(
    url: str,
    stop_event: Optional[object] = None,
    *,
    ttl: int = 300,
    timeout: int = 15,
    max_size: int = 1_000_000,
    allowed_schemes: Tuple[str, ...] = ("http", "https"),
    allowed_tlds: Optional[set[str]] = None,
    fetch: Callable[[str], ResponseLike] | None = None,
) -> Optional[str]:
    """Fetch ``url`` and return decoded text respecting several limits."""

    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in allowed_schemes:
        return None
    if allowed_tlds and parsed.hostname:
        tld = parsed.hostname.rsplit(".", 1)[-1].lower()
        if tld not in allowed_tlds:
            return None
    if fetch is not None:
        try:
            return fetch(url).text
        except Exception:
            return None
    now = time.time()
    cached = _CACHE.get(url)
    if cached and cached[0] > now:
        return cached[1]
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            final_url = getattr(resp, "geturl", lambda: url)()
            final_parsed = urllib.parse.urlparse(final_url)
            if final_parsed.scheme not in allowed_schemes:
                return None
            if parsed.netloc and final_parsed.netloc and parsed.netloc != final_parsed.netloc:
                return None
            if allowed_tlds and final_parsed.hostname:
                tld = final_parsed.hostname.rsplit(".", 1)[-1].lower()
                if tld not in allowed_tlds:
                    return None
            headers = getattr(resp, "headers", None)
            encoding = "utf-8"
            if headers and hasattr(headers, "get_content_charset"):
                encoding = headers.get_content_charset() or "utf-8"
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
    except TimeoutError:
        return None
    except Exception:
        try:
            with _fetch_stream("GET", url) as resp:
                final_url = str(getattr(resp, "url", url))
                final_parsed = urllib.parse.urlparse(final_url)
                if final_parsed.scheme not in allowed_schemes:
                    return None
                if parsed.netloc and final_parsed.netloc and parsed.netloc != final_parsed.netloc:
                    return None
                if allowed_tlds and final_parsed.hostname:
                    tld = final_parsed.hostname.rsplit(".", 1)[-1].lower()
                    if tld not in allowed_tlds:
                        return None
                encoding = getattr(resp, "encoding", None) or "utf-8"
                chunks = []
                total = 0
                for chunk in resp.iter_bytes(chunk_size=_READ_CHUNK):
                    if stop_event and getattr(stop_event, "is_set", lambda: False)():
                        return None
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


def fetch_bytes(
    url: str,
    stop_event: Optional[object] = None,
    *,
    ttl: int = 300,
    timeout: int = 15,
    max_size: int = 1_000_000,
    allowed_schemes: Tuple[str, ...] = ("http", "https"),
    fetch: Callable[[str], ResponseLike] | None = None,
) -> Optional[bytes]:
    """Fetch ``url`` and return raw bytes with caching."""

    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in allowed_schemes:
        return None
    if fetch is not None:
        try:
            return fetch(url).content
        except Exception:
            return None
    now = time.time()
    cached = _CACHE_BYTES.get(url)
    if cached and cached[0] > now:
        return cached[1]
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
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
    except TimeoutError:
        return None
    except Exception:
        try:
            with _fetch_stream("GET", url) as resp:
                chunks = []
                total = 0
                for chunk in resp.iter_bytes(chunk_size=_READ_CHUNK):
                    if stop_event and getattr(stop_event, "is_set", lambda: False)():
                        return None
                    chunks.append(chunk)
                    total += len(chunk)
                    if total >= max_size:
                        break
                data = b"".join(chunks)
        except Exception:  # pragma: no cover - network errors
            return None
    _CACHE_BYTES[url] = (now + ttl, data)
    return data


def _extract_from_json(obj, source_ref: str, stats: Dict[str, int]) -> List[EmailHit]:
    hits: List[EmailHit] = []
    if isinstance(obj, dict):
        for v in obj.values():
            hits.extend(_extract_from_json(v, source_ref, stats))
    elif isinstance(obj, list):
        for v in obj:
            hits.extend(_extract_from_json(v, source_ref, stats))
    elif isinstance(obj, str):
        text_candidates = [normalize_text(obj)]
        decoded = maybe_decode_base64(obj)
        if decoded:
            text_candidates.append(decoded)
        for text in text_candidates:
            for e in _SIMPLE_EMAIL_RE.findall(text):
                email = e.lower()
                email, _ = strip_phone_prefix(email, stats)
                hits.append(EmailHit(email=email, source_ref=source_ref, origin="ldjson"))
            hits.extend(extract_obfuscated_hits(text, source_ref, stats))
    return hits


def extract_ldjson_hits(html: str, base_url: str, stats: Dict[str, int]) -> List[EmailHit]:
    """Parse ``html`` for embedded JSON structures and extract emails."""

    hits: List[EmailHit] = []
    # <script type="application/ld+json"> ... </script>
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        flags=re.I | re.S,
    ):
        block = m.group(1)
        try:
            data = json.loads(block)
        except Exception:
            continue
        hits.extend(_extract_from_json(data, f"url:{base_url}", stats))
    # __NEXT_DATA__, window.__NUXT__, window.__INITIAL_STATE__
    for m in re.finditer(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html,
        flags=re.I | re.S,
    ):
        try:
            data = json.loads(m.group(1))
        except Exception:
            continue
        hits.extend(_extract_from_json(data, f"url:{base_url}", stats))
    for m in re.finditer(r'window\.__NUXT__\s*=\s*(\{.*?\});', html, flags=re.I | re.S):
        try:
            data = json.loads(m.group(1))
        except Exception:
            continue
        hits.extend(_extract_from_json(data, f"url:{base_url}", stats))
    for m in re.finditer(
        r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\});', html, flags=re.I | re.S
    ):
        try:
            data = json.loads(m.group(1))
        except Exception:
            continue
        hits.extend(_extract_from_json(data, f"url:{base_url}", stats))
    return hits


def extract_bundle_hits(
    html: str,
    base_url: str,
    stats: Dict[str, int],
    *,
    stop_event: Optional[object] = None,
    max_assets: int = 8,
    fetch: Callable[[str], ResponseLike] | None = None,
) -> List[EmailHit]:
    """Fetch JS bundle assets referenced in ``html`` and extract emails."""

    hits: List[EmailHit] = []
    srcs = re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', html, flags=re.I)
    count = 0
    for src in srcs:
        if count >= max_assets:
            break
        if stop_event and getattr(stop_event, "is_set", lambda: False)():
            stats["stop_interrupts"] = stats.get("stop_interrupts", 0) + 1
            break
        url = urllib.parse.urljoin(base_url, src)
        js = fetch_url(url, stop_event, fetch=fetch)
        if not js:
            continue
        count += 1
        stats["assets_scanned"] = stats.get("assets_scanned", 0) + 1
        source_ref = f"url:{url}"
        text = normalize_text(js)
        for e in extract_emails_document(text, stats):
            hits.append(EmailHit(email=e, source_ref=source_ref, origin="bundle"))
        hits.extend(extract_obfuscated_hits(text, source_ref, stats))
        for b64 in re.findall(r'atob\(["\']([^"\']+)["\']\)', js):
            decoded = maybe_decode_base64(b64)
            if decoded:
                for e in _SIMPLE_EMAIL_RE.findall(decoded):
                    email = e.lower()
                    email, _ = strip_phone_prefix(email, stats)
                    hits.append(EmailHit(email=email, source_ref=source_ref, origin="bundle"))
        for b64 in re.findall(
            r'Buffer\.from\(["\']([^"\']+)["\'],\s*["\']base64["\']\)', js
        ):
            decoded = maybe_decode_base64(b64)
            if decoded:
                for e in _SIMPLE_EMAIL_RE.findall(decoded):
                    email = e.lower()
                    email, _ = strip_phone_prefix(email, stats)
                    hits.append(EmailHit(email=email, source_ref=source_ref, origin="bundle"))
    return hits


_DOC_EXTS = {".pdf", ".docx", ".xlsx", ".csv", ".txt", ".html", ".htm"}


def extract_sitemap_hits(
    base_url: str,
    stats: Dict[str, int],
    *,
    stop_event: Optional[object] = None,
    max_urls: int = 200,
    max_docs: int = 30,
    fetch: Callable[[str], ResponseLike] | None = None,
) -> List[EmailHit]:
    """Fetch sitemap URLs and extract emails from listed documents."""

    hits: List[EmailHit] = []
    parsed_root = urllib.parse.urlparse(base_url)
    robots_url = urllib.parse.urljoin(base_url, "/robots.txt")
    robots = fetch_url(robots_url, stop_event, fetch=fetch)
    sitemap_urls: List[str] = []
    if robots:
        for line in robots.splitlines():
            if line.lower().startswith("sitemap:"):
                sitemap_urls.append(line.split(":", 1)[1].strip())
    if not sitemap_urls:
        sitemap_urls.append(urllib.parse.urljoin(base_url, "/sitemap.xml"))
    seen = 0
    for sm in sitemap_urls:
        if seen >= max_urls:
            break
        data = fetch_bytes(sm, stop_event, fetch=fetch)
        if not data:
            continue
        seen += 1
        stats["sitemap_urls_scanned"] = stats.get("sitemap_urls_scanned", 0) + 1
        try:
            from xml.etree import ElementTree as ET

            root = ET.fromstring(data)
        except Exception:
            continue
        for loc in root.findall(".//{*}loc"):
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                stats["stop_interrupts"] = stats.get("stop_interrupts", 0) + 1
                break
            url = loc.text or ""
            parsed = urllib.parse.urlparse(url)
            if parsed.hostname and parsed.hostname != parsed_root.hostname:
                continue
            ext = os.path.splitext(parsed.path)[1].lower()
            if ext not in _DOC_EXTS:
                continue
            if stats.get("docs_parsed", 0) >= max_docs:
                break
            data_doc = fetch_bytes(url, stop_event, fetch=fetch)
            if not data_doc:
                continue
            stats["docs_parsed"] = stats.get("docs_parsed", 0) + 1
            from .extraction import extract_any_stream

            hits_doc, _ = extract_any_stream(
                data_doc, ext, source_ref=f"url:{url}", stop_event=stop_event
            )
            for h in hits_doc:
                hits.append(EmailHit(email=h.email, source_ref=h.source_ref, origin="document"))
    if hits:
        stats["hits_sitemap"] = stats.get("hits_sitemap", 0) + len(hits)
    return hits


def extract_api_hits(
    html: str,
    base_url: str,
    stats: Dict[str, int],
    *,
    stop_event: Optional[object] = None,
    max_docs: int = 30,
    fetch: Callable[[str], ResponseLike] | None = None,
) -> List[EmailHit]:
    """Scan heuristic document endpoints in ``html`` and extract emails."""

    hits: List[EmailHit] = []
    links = re.findall(
        r'(?:href|src|data-href)=["\']([^"\']+)["\']', html, flags=re.I
    )
    patterns = re.compile(r"/api/files/document/|/download/|/media/|/content/", re.I)
    count = 0
    for href in links:
        if count >= max_docs:
            break
        if not patterns.search(href):
            continue
        url = urllib.parse.urljoin(base_url, href)
        ext = os.path.splitext(urllib.parse.urlparse(url).path)[1].lower()
        if ext not in _DOC_EXTS:
            continue
        data = fetch_bytes(url, stop_event, fetch=fetch)
        if not data:
            continue
        count += 1
        stats["docs_parsed"] = stats.get("docs_parsed", 0) + 1
        from .extraction import extract_any_stream

        hits_doc, _ = extract_any_stream(
            data, ext, source_ref=f"url:{url}", stop_event=stop_event
        )
        for h in hits_doc:
            hits.append(EmailHit(email=h.email, source_ref=h.source_ref, origin="document"))
    if hits:
        stats["hits_api"] = stats.get("hits_api", 0) + len(hits)
    return hits


def extract_obfuscated_hits(
    text: str, source_ref: str, stats: Optional[Dict[str, int]] = None
) -> List[EmailHit]:
    """Return all ``EmailHit`` objects found via obfuscation patterns."""

    strict = get("STRICT_OBFUSCATION", settings.STRICT_OBFUSCATION)
    radius = get("FOOTNOTE_RADIUS_PAGES", settings.FOOTNOTE_RADIUS_PAGES)
    layout = get("PDF_LAYOUT_AWARE", settings.PDF_LAYOUT_AWARE)
    ocr = get("ENABLE_OCR", settings.ENABLE_OCR)
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
        if strict and local.isdigit():
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


__all__ = [
    "extract_obfuscated_hits",
    "fetch_url",
    "fetch_bytes",
    "ResponseLike",
    "decode_cfemail",
    "extract_ldjson_hits",
    "extract_bundle_hits",
    "extract_sitemap_hits",
    "extract_api_hits",
]

