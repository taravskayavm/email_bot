"""Helpers for deduplication of e-mail hits."""

from __future__ import annotations

import unicodedata
from typing import Dict, List, Tuple, TYPE_CHECKING

from . import settings

if TYPE_CHECKING:  # pragma: no cover - for type checkers only
    from .extraction import EmailHit


_SUPER_DIGITS = set("⁰¹²³⁴⁵⁶⁷⁸⁹")


def is_superscript_digit(ch: str) -> bool:
    """Return ``True`` if ``ch`` is a Unicode superscript digit."""

    return ch in _SUPER_DIGITS


def _is_superscript(ch: str) -> bool:
    return is_superscript_digit(ch) or "SUPERSCRIPT" in unicodedata.name(ch, "")


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


def merge_footnote_prefix_variants(hits: List["EmailHit"], stats: Dict[str, int] | None = None) -> List["EmailHit"]:
    """Merge footnote-trimmed variants of the same e-mail within one source."""

    if stats is None:
        stats = {}

    grouped: Dict[str, List[Tuple[int, "EmailHit", int]]] = {}
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
                stats["footnote_pairs_merged"] = stats.get("footnote_pairs_merged", 0) + 1

    return [h for idx, h in enumerate(hits) if idx not in removed]


def repair_footnote_singletons(
    hits: List["EmailHit"], layout_aware: bool = False
) -> Tuple[List["EmailHit"], Dict[str, int]]:
    """Repair or guard against stray footnote markers inside e-mails."""

    from .extraction import EmailHit  # local import to avoid circular deps

    stats: Dict[str, int] = {
        "footnote_singletons_repaired": 0,
        "footnote_guard_skips": 0,
        "footnote_ambiguous_kept": 0,
    }
    all_emails = {h.email for h in hits}
    out: List[EmailHit] = []
    for h in hits:
        if h.meta.get("repaired"):
            out.append(h)
            continue
        prev = _last_visible(h.pre)
        if not prev:
            out.append(h)
            continue
        local, dom = h.email.split("@", 1)

        if is_superscript_digit(prev):
            if not local:
                out.append(h)
                stats["footnote_guard_skips"] += 1
                continue
            rest = local[1:]
            new_meta = dict(h.meta)
            new_meta["repaired"] = True
            out.append(
                EmailHit(
                    email=f"{rest}@{dom}",
                    source_ref=h.source_ref,
                    origin="footnote_repaired",
                    pre=h.pre,
                    post=h.post,
                    meta=new_meta,
                )
            )
            stats["footnote_singletons_repaired"] += 1
            continue

        if not layout_aware and prev.isalnum():
            if len(local) >= 2:
                trimmed = f"{local[1:]}@{dom}"
                if trimmed in all_emails:
                    continue
            out.append(h)
            stats["footnote_ambiguous_kept"] += 1
            continue

        out.append(h)
    return out, stats

__all__ = [
    "merge_footnote_prefix_variants",
    "repair_footnote_singletons",
    "is_superscript_digit",
]

