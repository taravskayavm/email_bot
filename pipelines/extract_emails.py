from __future__ import annotations

from typing import Dict, List, Tuple

from utils.email_clean import (
    classify_email_role,
    dedupe_with_variants,
    parse_emails_unified,
)
from utils.name_match import extract_names, fio_match_score


def extract_emails_pipeline(text: str) -> Tuple[List[str], Dict[str, int]]:
    """High-level pipeline that applies deobfuscation, normalization and dedupe."""

    cleaned, meta = parse_emails_unified(text or "", return_meta=True)
    unique = dedupe_with_variants(cleaned)

    items = meta.get("items", [])
    deobf_count = meta.get("deobfuscated_count", 0)
    conf_count = meta.get("confusables_fixed", 0)
    dropped = sum(1 for item in items if not item.get("sanitized"))

    role_stats: Dict[str, int] = {"personal": 0, "role": 0}
    ctx = text or ""
    classified: Dict[str, Dict[str, object]] = {}
    for addr in unique:
        if "@" not in addr:
            continue
        local, domain = addr.split("@", 1)
        info = classify_email_role(local, domain, ctx)
        info_class = str(info.get("class") or "role")
        if info_class not in role_stats:
            role_stats[info_class] = 0
        role_stats[info_class] += 1
        classified[addr] = info

    # [EBOT-PARSER-FIO-003] FIO matching
    names = extract_names(text or "")
    fio_scores: Dict[str, float] = {}
    for addr in unique:
        if "@" not in addr:
            fio_scores[addr] = 0.0
            continue
        local = addr.split("@", 1)[0]
        fio_scores[addr] = max([fio_match_score(local, fio) for fio in names] or [0.0])

    meta["role_stats"] = role_stats
    meta["classified"] = classified
    meta["fio_scores"] = fio_scores

    stats = {
        "found_raw": len(items),
        "after_deobfuscation": deobf_count,
        "normalized_confusables": conf_count,
        "final_unique": len(unique),
        "dropped": dropped,
        "deobfuscated_count": deobf_count,
        "confusables_fixed": conf_count,
        "personal": role_stats.get("personal", 0),
        "role": role_stats.get("role", 0),
        "classified": classified,
        "has_fio": 1 if names else 0,
    }

    return unique, stats


def run_pipeline_on_text(text: str) -> Tuple[List[str], List[Tuple[str, str]]]:
    """Convenience helper returning final e-mails and dropped candidates."""

    cleaned, meta = parse_emails_unified(text or "", return_meta=True)
    final = dedupe_with_variants(cleaned)
    dropped: List[Tuple[str, str]] = []
    for item in meta.get("items", []):
        if item.get("sanitized"):
            continue
        candidate = str(item.get("raw") or item.get("normalized") or "")
        reason = str(item.get("reason") or "invalid")
        if candidate:
            dropped.append((candidate, reason))
    return final, dropped


__all__ = ["extract_emails_pipeline", "run_pipeline_on_text"]
