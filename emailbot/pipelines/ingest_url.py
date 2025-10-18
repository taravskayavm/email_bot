"""Helpers to fetch and extract e-mail addresses from URLs."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from emailbot.reporting import count_blocked
from pipelines.extract_emails import extract_from_url_async


async def ingest_url(
    url: str,
    *,
    deep: bool = False,
    path_prefixes: Optional[list[str]] = None,
    limit_pages: Optional[int] = None,
) -> Tuple[List[str], Dict[str, int]]:
    """Fetch ``url`` and return extracted e-mails along with summary stats."""

    emails, meta = await extract_from_url_async(
        url,
        deep=deep,
        path_prefixes=path_prefixes,
        max_pages=limit_pages if deep else None,
    )
    ok = list(dict.fromkeys(emails or []))
    try:
        blocked_count = count_blocked(ok)
    except Exception:
        blocked_count = 0

    stats: Dict[str, int] = {
        "total_in": 0,
        "ok": len(ok),
        "blocked": blocked_count,
    }
    if isinstance(meta, dict):
        items = meta.get("items")
        if isinstance(items, list):
            stats["total_in"] = len(items)
        else:
            found_raw = meta.get("found_raw")
            if isinstance(found_raw, int):
                stats["total_in"] = found_raw
        pages_total = meta.get("pages")
        if isinstance(pages_total, int):
            stats["pages"] = pages_total
        pages_limit = meta.get("pages_limit")
        if isinstance(pages_limit, int):
            stats["pages_limit"] = pages_limit
    if not stats["total_in"]:
        stats["total_in"] = len(ok)
    return ok, stats


__all__ = ["ingest_url"]

