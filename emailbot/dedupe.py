"""Helpers for deduplication of e-mail hits."""

from __future__ import annotations

import unicodedata
from typing import Dict, List, Tuple, TYPE_CHECKING

from . import settings

if TYPE_CHECKING:  # pragma: no cover - for type checkers only
    from .extraction import EmailHit


_SUPER_DIGITS = set("⁰¹²³⁴⁵⁶⁷⁸⁹")


def _is_superscript(ch: str) -> bool:
    return ch in _SUPER_DIGITS or "SUPERSCRIPT" in unicodedata.name(ch, "")


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
                stats["footnote_trimmed_merged"] = stats.get("footnote_trimmed_merged", 0) + 1

    return [h for idx, h in enumerate(hits) if idx not in removed]


def repair_footnote_singletons(
    hits: List["EmailHit"], layout_aware: bool = False
) -> Tuple[List["EmailHit"], int]:
    """Fix leading footnote digits duplicated in the local part.

    Works only for PDF-derived hits where ``pre`` ends with a superscript digit
    (``¹²³⁴⁵⁶⁷⁸⁹⁰``).  If the local part begins with the same digit (or letter),
    removes exactly one leading character provided the remaining part looks like
    a plausible address (length ≥ 3 and contains at least one ASCII letter).

    When ``layout_aware`` is true, a regular digit to the left also qualifies as
    a footnote marker.
    """

    from .extraction import EmailHit  # local import to avoid circular deps

    out: List[EmailHit] = []
    fixed = 0
    for h in hits:
        if h.origin == "footnote_repaired":
            out.append(h)
            continue

        ref = h.source_ref.lower()
        if ref.startswith("zip:"):
            if "|" not in ref:
                out.append(h)
                continue
            inner = ref.split("|", 1)[1].split("#", 1)[0].lower()
            if not inner.endswith(".pdf"):
                out.append(h)
                continue
        elif not ref.startswith("pdf:"):
            out.append(h)
            continue

        prev = _last_visible(h.pre)
        if not prev:
            out.append(h)
            continue
        if prev not in _SUPER_DIGITS:
            if not (layout_aware and prev.isdigit()):
                out.append(h)
                continue

        local, dom = h.email.split("@", 1)
        if not local or not local[0].isalnum():
            out.append(h)
            continue

        first = local[0]
        if prev in _SUPER_DIGITS or prev.isdigit():
            try:
                if unicodedata.digit(prev) != unicodedata.digit(first):
                    out.append(h)
                    continue
            except Exception:
                out.append(h)
                continue

        rest = local[1:]
        if len(rest) < 3 or not any("A" <= c <= "Z" or "a" <= c <= "z" for c in rest):
            out.append(h)
            continue

        new_email = f"{rest}@{dom}"
        out.append(
            EmailHit(
                email=new_email,
                source_ref=h.source_ref,
                origin="footnote_repaired",
                pre=h.pre,
                post=h.post,
            )
        )
        fixed += 1

    return out, fixed


__all__ = ["merge_footnote_prefix_variants", "repair_footnote_singletons"]

