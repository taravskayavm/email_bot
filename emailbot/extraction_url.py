"""Helpers for extracting e-mail hits from URLs."""

from __future__ import annotations

import json
import os
import re
import time
import urllib.parse
import urllib.request
from typing import Callable, Dict, List, Optional, Protocol, Tuple

try:  # pragma: no cover - optional dependency
    import httpx  # type: ignore
except Exception:  # pragma: no cover
    httpx = None  # type: ignore[assignment]

from .extraction import (
    EmailHit,
    _strip_left_noise,
    _valid_local,
    _valid_domain,
    _is_suspicious_local,
    extract_emails_document,
)
from .extraction_common import (
    normalize_domain,
    normalize_text,
    maybe_decode_base64,
    strip_phone_prefix,
)
from emailbot import settings
from emailbot.settings_store import get
from .run_control import should_stop
from .progress_watchdog import heartbeat_now

# Локаль: 1–64 символа из допустимого набора (минимум и наличие буквы проверяем отдельно).
_LOCAL_BASE = r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]{1,64}"

# Доменный лейбл: 1–63, не начинается/заканчивается дефисом, и содержит хотя бы одну букву.
_LABEL_STRICT = (
    r"(?=[\w-]*[^\W\d_])"
    r"[\w](?:[\w-]{0,61}[\w])?"
)

# Обфускация: local (at|@|собака) label (dot label)* — собираем паттерн без f-строк.
_DOT_VARIANT = r"(?:\.|dot|\(dot\)|\[dot\]|\{dot\}|точка|ponto|tochka)"
_OBFUSCATED_PATTERN = "".join(
    [
        r"(?xi)",
        r"(?P<local>",
        _LOCAL_BASE,
        r")",
        r"\s*",
        r"(?P<at>",
        r"@",
        r"| \(at\) | \[at\] | \{at\} | \bat\b",
        r"| собака | собакa | arroba | sobaka",
        r")",
        r"\s*",
        r"(?P<domain>",
        r"(?:",
        _LABEL_STRICT,
        r"(?:\s*",
        _DOT_VARIANT,
        r"\s*",
        _LABEL_STRICT,
        r")+",
        r")",
        r")",
    ]
)
_OBFUSCATED_RE = re.compile(_OBFUSCATED_PATTERN, re.IGNORECASE | re.VERBOSE | re.UNICODE)

_DOT_SPLIT_RE = re.compile(r"\s*" + _DOT_VARIANT + r"\s*", re.I)

_SOB_WORD_RE = re.compile(
    r"(?<!\w)(?:[\[\(\{]\s*)?(?:[сcs][оo0](?:б|b)[аa@][кk][аa@])(?:\s*[\]\)\}])?(?!\w)",
    re.IGNORECASE,
)
_SOB_WORD_ATTACHED_RE = re.compile(
    r"(?<=\w)(?:[\[\(\{]\s*)?(?:[сcs][оo0](?:б|b)[аa@][кk][аa@])(?:\s*[\]\)\}])?(?=\w)",
    re.IGNORECASE,
)
_DOT_WORD_RE = re.compile(
    r"(?<!\w)(?:[\[\(\{]\s*)?(?:[тt][оo0](?:ч|ch)[кk][аa@]|dot|ponto)(?:\s*[\]\)\}])?(?!\w)",
    re.IGNORECASE,
)
_DOT_WORD_ATTACHED_RE = re.compile(
    r"(?<=\w)(?:[\[\(\{]\s*)?(?:[тt][оo0](?:ч|ch)[кk][аa@]|dot|ponto)(?:\s*[\]\)\}])?(?=\w)",
    re.IGNORECASE,
)

_CACHE: Dict[str, Tuple[float, str]] = {}
_CACHE_BYTES: Dict[str, Tuple[float, bytes]] = {}
_CURRENT_BATCH: str | None = None
_READ_CHUNK = 128 * 1024
_DEFAULT_FETCH_HEADERS = {
    "User-Agent": "emailbot/1.0 (+parser)",
    "Accept": "text/*,application/pdf,application/xml,application/msword,application/vnd.openxmlformats-officedocument.*;q=0.9,*/*;q=0.1",
}

_ALLOWED_CONTENT_TYPES = (
    "text/",
    "pdf",
    "xml",
    "html",
    "msword",
    "officedocument",
    "json",
    "javascript",
)
_SIMPLE_EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._%+\-])[A-Za-z0-9._%+\-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
)


def _is_allowed_content_type(value: str) -> bool:
    if not value:
        return True
    lowered = value.lower()
    return any(hint in lowered for hint in _ALLOWED_CONTENT_TYPES)


def _fetch_get(url: str, *, timeout: int = 15, headers: dict[str, str] | None = None):
    if httpx is None:  # pragma: no cover - guarded by caller
        raise RuntimeError("optional dependency 'httpx' is not installed")
    return httpx.get(url, timeout=timeout, headers=headers)


def _fetch_stream(method: str, url: str, *, timeout: int = 15, headers: dict[str, str] | None = None):
    if httpx is None:  # pragma: no cover - guarded by caller
        raise RuntimeError("optional dependency 'httpx' is not installed")
    return httpx.stream(method, url, timeout=timeout, headers=headers)


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

    if httpx is None:
        raise RuntimeError("optional dependency 'httpx' is not installed")

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
    request_headers = dict(_DEFAULT_FETCH_HEADERS)
    now = time.time()
    cached = _CACHE.get(url)
    if cached and cached[0] > now:
        return cached[1]
    try:
        req = urllib.request.Request(url, method="GET", headers=request_headers)
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
            resp_headers = getattr(resp, "headers", None)
            encoding = "utf-8"
            if resp_headers and hasattr(resp_headers, "get_content_charset"):
                encoding = resp_headers.get_content_charset() or "utf-8"
            if resp_headers:
                header_get = getattr(resp_headers, "get", None)
                content_length = header_get("Content-Length") if header_get else None
                if content_length:
                    try:
                        if int(content_length) > max_size:
                            return None
                    except ValueError:
                        pass
                content_type = header_get("Content-Type", "") if header_get else ""
                if content_type and not _is_allowed_content_type(content_type):
                    return None
            chunks: List[bytes] = []
            total = 0
            while True:
                if should_stop() or (
                    stop_event and getattr(stop_event, "is_set", lambda: False)()
                ):
                    return None
                chunk = resp.read(_READ_CHUNK)
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total >= max_size:
                    break
            data = b"".join(chunks)
            text_out = data.decode(encoding, "ignore")
    except TimeoutError:
        return None
    except Exception:
        try:
            with _fetch_stream("GET", url, timeout=timeout, headers=request_headers) as resp:
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
                resp_headers = getattr(resp, "headers", None)
                header_get = getattr(resp_headers, "get", None)
                content_length = header_get("Content-Length") if header_get else None
                if content_length:
                    try:
                        if int(content_length) > max_size:
                            return None
                    except ValueError:
                        pass
                content_type = header_get("Content-Type", "") if header_get else ""
                if content_type and not _is_allowed_content_type(content_type):
                    return None
                encoding = getattr(resp, "encoding", None) or "utf-8"
                chunks = []
                total = 0
                for chunk in resp.iter_bytes(chunk_size=_READ_CHUNK):
                    if should_stop() or (
                        stop_event and getattr(stop_event, "is_set", lambda: False)()
                    ):
                        return None
                    chunks.append(chunk)
                    total += len(chunk)
                    if total >= max_size:
                        break
                data = b"".join(chunks)
                text_out = data.decode(encoding, "ignore")
        except Exception:  # pragma: no cover - network errors
            return None
    _CACHE[url] = (now + ttl, text_out)
    return text_out



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
    request_headers = dict(_DEFAULT_FETCH_HEADERS)
    now = time.time()
    cached = _CACHE_BYTES.get(url)
    if cached and cached[0] > now:
        return cached[1]
    try:
        req = urllib.request.Request(url, method="GET", headers=request_headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp_headers = getattr(resp, "headers", None)
            if resp_headers:
                header_get = getattr(resp_headers, "get", None)
                content_length = header_get("Content-Length") if header_get else None
                if content_length:
                    try:
                        if int(content_length) > max_size:
                            return None
                    except ValueError:
                        pass
                content_type = header_get("Content-Type", "") if header_get else ""
                if content_type and not _is_allowed_content_type(content_type):
                    return None
            chunks: List[bytes] = []
            total = 0
            while True:
                if should_stop() or (
                    stop_event and getattr(stop_event, "is_set", lambda: False)()
                ):
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
            with _fetch_stream("GET", url, timeout=timeout, headers=request_headers) as resp:
                resp_headers = getattr(resp, "headers", None)
                header_get = getattr(resp_headers, "get", None)
                content_length = header_get("Content-Length") if header_get else None
                if content_length:
                    try:
                        if int(content_length) > max_size:
                            return None
                    except ValueError:
                        pass
                content_type = header_get("Content-Type", "") if header_get else ""
                if content_type and not _is_allowed_content_type(content_type):
                    return None
                chunks = []
                total = 0
                for chunk in resp.iter_bytes(chunk_size=_READ_CHUNK):
                    if should_stop() or (
                        stop_event and getattr(stop_event, "is_set", lambda: False)()
                    ):
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
        if should_stop() or (
            stop_event and getattr(stop_event, "is_set", lambda: False)()
        ):
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
        heartbeat_now()
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
            if should_stop() or (
                stop_event and getattr(stop_event, "is_set", lambda: False)()
            ):
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
        heartbeat_now()
        if count >= max_docs:
            break
        if not patterns.search(href):
            continue
        if should_stop() or (
            stop_event and getattr(stop_event, "is_set", lambda: False)()
        ):
            stats["stop_interrupts"] = stats.get("stop_interrupts", 0) + 1
            break
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

    radius = get("FOOTNOTE_RADIUS_PAGES", settings.FOOTNOTE_RADIUS_PAGES)
    layout = get("PDF_LAYOUT_AWARE", settings.PDF_LAYOUT_AWARE)
    ocr = get("ENABLE_OCR", settings.ENABLE_OCR)
    hits: List[EmailHit] = []
    for m in re.finditer(r'href=["\']mailto:([^"\'?]+)', text, flags=re.I):
        addr = urllib.parse.unquote(m.group(1)).strip()
        if not addr or "@" not in addr:
            continue
        local_raw, domain_raw = addr.split("@", 1)
        local = local_raw.strip().lower()
        domain_raw = domain_raw.strip()
        if not local or not domain_raw:
            continue
        ascii_domain = normalize_domain(domain_raw)
        if not ascii_domain:
            continue
        if not (_valid_local(local) and _valid_domain(ascii_domain)):
            continue
        email = f"{local}@{ascii_domain}".lower()
        hits.append(EmailHit(email=email, source_ref=source_ref, origin="mailto"))
        if stats is not None:
            stats["hits_mailto"] = stats.get("hits_mailto", 0) + 1

    text = normalize_text(text)
    text = _SOB_WORD_ATTACHED_RE.sub(" at ", text)
    text = _SOB_WORD_RE.sub(" at ", text)
    text = _DOT_WORD_ATTACHED_RE.sub(" dot ", text)
    text = _DOT_WORD_RE.sub(" dot ", text)
    allow_numeric_local = os.getenv("OBFUSCATION_ALLOW_NUMERIC_LOCAL", "0").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    for m in _OBFUSCATED_RE.finditer(text):
        local = m.group("local")
        domain_raw = m.group("domain")
        parts = [p for p in _DOT_SPLIT_RE.split(domain_raw) if p]
        if not parts:
            continue
        domain = ".".join(parts).lower()

        local, _ = strip_phone_prefix(local, stats)

        start, end = m.span()
        pre = text[max(0, start - 16) : start]
        post = text[end : end + 16]
        local, _ = _strip_left_noise(local, pre, stats)

        # Базовая проверка формата (включая IDNA/TLD и т.п.)
        if not (_valid_local(local) and _valid_domain(domain)):
            continue
        # Усиленные фильтры против мусора:
        # 1) Локаль не должна быть односимвольной.
        if len(local) < 2:
            if stats is not None:
                stats["numeric_from_obfuscation_dropped"] = stats.get(
                    "numeric_from_obfuscation_dropped", 0
                ) + 1
            continue
        if len(local) > 64:
            continue
        # 1a) По умолчанию локаль должна содержать хотя бы одну букву.
        if (not allow_numeric_local) and not re.search(r"[A-Za-z]", local):
            if stats is not None:
                stats["numeric_from_obfuscation_dropped"] = stats.get(
                    "numeric_from_obfuscation_dropped", 0
                ) + 1
            continue
        if "." not in domain:
            continue
        tld = domain.rsplit(".", 1)[-1]
        if not (2 <= len(tld) <= 24):
            continue
        labels_ok = True
        for lbl in domain.split("."):
            if not re.search(r"[A-Za-z]", lbl):
                labels_ok = False
                break
        if not labels_ok:
            continue
        email = f"{local}@{domain}".lower()

        if _is_suspicious_local(local):
            if stats is not None:
                stats["quarantined"] = stats.get("quarantined", 0) + 1
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

