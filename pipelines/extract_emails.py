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


__all__ = ["extract_emails_pipeline"]
