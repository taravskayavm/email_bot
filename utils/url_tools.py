"""Helpers for working with URLs inside the crawler."""

from __future__ import annotations

from urllib.parse import urljoin, urlparse, urlunparse, urldefrag


def canonicalize(base: str, href: str) -> str | None:
    """Resolve ``href`` against ``base`` and return a normalized absolute URL."""

    if not href:
        return None
    candidate = urljoin(base, href.strip())
    candidate, _fragment = urldefrag(candidate)
    parsed = urlparse(candidate)
    if not parsed.scheme or not parsed.netloc:
        return None
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
    )
    return urlunparse(normalized)


def same_domain(a: str, b: str) -> bool:
    """Return ``True`` if URLs ``a`` and ``b`` share the same hostname."""

    pa, pb = urlparse(a), urlparse(b)
    return (pa.hostname or "") == (pb.hostname or "")

