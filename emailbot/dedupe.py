"""Helpers for deduplication of e-mail hits."""

from __future__ import annotations

import unicodedata
from typing import Dict, List, Tuple

from . import settings
from .extraction_url import EmailHit


def _is_superscript(ch: str) -> bool:
    return "SUPERSCRIPT" in unicodedata.name(ch, "")


def _last_visible(s: str) -> str:
    for c in reversed(s):
        if not c.isspace():
            return c
    return ""


def _split_ref(ref: str) -> Tuple[str, int]:
    parts = ref.split("|")
    try:
        page = int(parts[-1])
        base = "|".join(parts[:-1])
    except ValueError:
        base, page = ref, 0
    return base, page


def merge_footnote_prefix_variants(hits: List[EmailHit], stats: Dict[str, int] | None = None) -> List[EmailHit]:
    """Merge footnote-trimmed variants of the same e-mail within one source."""

    if stats is None:
        stats = {}

    grouped: Dict[str, List[Tuple[int, EmailHit, int]]] = {}
    for idx, h in enumerate(hits):
        base, page = _split_ref(h.source_ref)
        grouped.setdefault(base, []).append((idx, h, page))

    removed: set[int] = set()

    for base, lst in grouped.items():
        for i, (idx_long, long, page_long) in enumerate(lst):
            loc_long, dom_long = long.email.split("@", 1)
            for j, (idx_short, short, page_short) in enumerate(lst):
                if i == j or idx_short in removed or idx_long in removed:
                    continue
                if abs(page_long - page_short) > settings.FOOTNOTE_RADIUS_PAGES:
                    continue
                loc_short, dom_short = short.email.split("@", 1)
                if dom_long != dom_short:
                    continue
                if len(loc_long) != len(loc_short) + 1:
                    continue
                if not loc_long.endswith(loc_short):
                    continue
                added = loc_long[0]
                if not (added.isalnum() or _is_superscript(added)):
                    continue
                prev_short = _last_visible(short.pre)
                prev_long = _last_visible(long.pre)
                cond = False
                if prev_short and (prev_short.isdigit() or _is_superscript(prev_short)):
                    cond = True
                if prev_long and (prev_long.isdigit() or _is_superscript(prev_long)):
                    cond = True
                if not cond:
                    continue
                removed.add(idx_short)
                stats["footnote_trimmed_merged"] = stats.get("footnote_trimmed_merged", 0) + 1

    return [h for idx, h in enumerate(hits) if idx not in removed]


__all__ = ["merge_footnote_prefix_variants"]

