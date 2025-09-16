from __future__ import annotations

from typing import Dict, List, Tuple

from utils.email_clean import dedupe_with_variants, parse_emails_unified


def extract_emails_pipeline(text: str) -> Tuple[List[str], Dict[str, int]]:
    """High-level pipeline that applies deobfuscation, normalization and dedupe."""

    cleaned, meta = parse_emails_unified(text or "", return_meta=True)
    unique = dedupe_with_variants(cleaned)

    items = meta.get("items", [])
    deobf_count = meta.get("deobfuscated_count", 0)
    conf_count = meta.get("confusables_fixed", 0)
    dropped = sum(1 for item in items if not item.get("sanitized"))

    stats = {
        "found_raw": len(items),
        "after_deobfuscation": deobf_count,
        "normalized_confusables": conf_count,
        "final_unique": len(unique),
        "dropped": dropped,
        "deobfuscated_count": deobf_count,
        "confusables_fixed": conf_count,
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
