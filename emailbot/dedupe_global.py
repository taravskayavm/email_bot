from __future__ import annotations

from typing import Dict, Iterable, List, Mapping, Tuple

try:
    # проект уже использует normalize в extraction_common
    from .extraction_common import normalize_email as _normalize
except Exception:  # pragma: no cover - fallback when optional deps missing
    def _normalize(value: str) -> str:
        return (value or "").strip().lower()


def dedupe_across_sources(
    hits: Iterable[dict],
) -> Tuple[List[dict], Dict[str, List[dict]]]:
    """Return ``(unique_hits, dup_map)`` grouped by normalized e-mail.

    ``dup_map`` uses the normalized e-mail as the key with the list of duplicate
    entries (excluding the first occurrence) as the value.
    """

    seen: Dict[str, dict] = {}
    duplicates: Dict[str, List[dict]] = {}
    unique: List[dict] = []

    for hit in hits:
        if not isinstance(hit, dict):
            continue
        email = _normalize(str(hit.get("email", "")))
        if not email:
            continue
        if email in seen:
            duplicates.setdefault(email, []).append(hit)
            continue
        seen[email] = hit
        unique.append(hit)

    return unique, duplicates


def build_hits_with_sources(
    emails: Iterable[str],
    source_map: Mapping[str, Iterable[str]] | None = None,
    *,
    fallback_source: str = "",
) -> List[dict]:
    """Expand ``emails`` into hit dictionaries for global dedupe.

    ``source_map`` is expected to map normalized e-mail addresses to one or
    more textual source identifiers.  When multiple sources are present for the
    same address, each additional source will be represented as a separate hit
    so that :func:`dedupe_across_sources` can report duplicates accurately.
    """

    if source_map is None:
        source_map = {}

    hits: List[dict] = []
    for email in emails:
        if not email:
            continue
        norm = _normalize(str(email))
        if not norm:
            continue
        sources = list(source_map.get(norm, []))
        if not sources:
            sources = [fallback_source]
        used: set[str] = set()
        for source in sources:
            source_text = str(source) if source is not None else ""
            if source_text in used:
                continue
            used.add(source_text)
            hits.append({"email": email, "source": source_text})
        # Ensure at least one hit even if all sources were empty/duplicates
        if not used:
            hits.append({"email": email, "source": fallback_source})

    return hits


__all__ = ["dedupe_across_sources", "build_hits_with_sources"]
