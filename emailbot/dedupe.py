"""Helpers for deduplication of e-mail hits."""

from __future__ import annotations

import unicodedata
from typing import Dict, List, Tuple, TYPE_CHECKING

from . import settings

if TYPE_CHECKING:  # pragma: no cover - for type checkers only
    from .extraction import EmailHit


_SUPER_TO_DIGIT = {
    "\u00b9": "1",
    "\u00b2": "2",
    "\u00b3": "3",
    "\u2070": "0",
    "\u2074": "4",
    "\u2075": "5",
    "\u2076": "6",
    "\u2077": "7",
    "\u2078": "8",
    "\u2079": "9",
}


def is_superscript_digit(ch: str) -> bool:
    """Return ``True`` if ``ch`` is a Unicode superscript digit."""

    return ch in _SUPER_TO_DIGIT


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

    for _base, lst in grouped.items():
        # Group by domain first to avoid redundant cross-domain checks.
        by_domain: dict[str, list[tuple[int, "EmailHit", int, str]]] = {}
        for idx, hit, page in lst:
            local, dom = hit.email.split("@", 1)
            by_domain.setdefault(dom, []).append((idx, hit, page, local))

        for domain_items in by_domain.values():
            short_map: dict[str, list[tuple[int, "EmailHit", int]]] = {}
            long_items: list[tuple[int, "EmailHit", int, str]] = []
            for idx, hit, page, local in domain_items:
                if local:
                    short_map.setdefault(local, []).append((idx, hit, page))
                long_items.append((idx, hit, page, local))

            for idx_long, long, page_long, loc_long in long_items:
                if len(loc_long) < 2:
                    continue
                loc_short = loc_long[1:]
                candidates = short_map.get(loc_short)
                if not candidates:
                    continue
                for idx_short, short, page_short in candidates:
                    if idx_short == idx_long or idx_short in removed or idx_long in removed:
                        continue
                    if abs(page_long - page_short) > settings.FOOTNOTE_RADIUS_PAGES:
                        continue
                    added = loc_long[0]
                    # Раньше «сносочным» считался любой буквенно-цифровой префикс,
                    # из-за чего отрезались первые буквы (a/b/c) у реальных адресов.
                    # Теперь разрешаем только цифровые маркеры (в т.ч. надстрочные).
                    if not (added.isdigit() or _is_superscript(added)):
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
            digit = _SUPER_TO_DIGIT.get(prev, "")
            if local and local[0] == digit:
                rest = local[1:]
                if len(rest) >= 3 and any(c.isalpha() for c in rest):
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
            stats["footnote_guard_skips"] += 1
            out.append(h)
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

