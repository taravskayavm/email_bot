from __future__ import annotations

import os
from typing import Dict, List, Tuple

from utils.email_clean import dedupe_with_variants, parse_emails_unified
from utils.email_role import classify_email_role
from utils.name_match import extract_names, fio_match_score

PERSONAL_ONLY = os.getenv("EMAIL_ROLE_PERSONAL_ONLY", "1") == "1"


def extract_emails_pipeline(text: str) -> Tuple[List[str], Dict[str, int]]:
    """High-level pipeline that applies deobfuscation, normalization and dedupe."""

    raw_text = text or ""
    cleaned, meta = parse_emails_unified(raw_text, return_meta=True)
    unique = dedupe_with_variants(cleaned)

    items = meta.get("items", [])
    deobf_count = meta.get("deobfuscated_count", 0)
    conf_count = meta.get("confusables_fixed", 0)
    dropped = sum(1 for item in items if not item.get("sanitized"))

    # [EBOT-PIPELINE-CONTEXT-004] naive source_context from nearby keywords
    def guess_context(around: str) -> str:
        s = (around or "").lower()
        if any(k in s for k in ("mailto:",)):
            return "mailto"
        if any(k in s for k in ("corresponding author", "corresponding", "корреспондир")):
            return "pdf_corresponding_author"
        if any(k in s for k in ("author", "автор")):
            return "author_block"
        if any(k in s for k in ("footer", "подвал", "copyright")):
            return "footer"
        if any(k in s for k in ("contact", "контакт", "связь")):
            return "contacts"
        return "unknown"

    window = 180  # chars around email for context
    contexts: Dict[str, str] = {}
    snippets: Dict[str, str] = {}
    per_address_infos: Dict[str, List[Dict[str, object]]] = {}

    def slice_with_context(span: Tuple[int, int] | None) -> str:
        if not raw_text:
            return ""
        if not span:
            return raw_text
        start, end = span
        left = max(0, start - window)
        right = min(len(raw_text), end + window)
        return raw_text[left:right]

    for item in items:
        normalized = (item.get("normalized") or "").strip()
        sanitized = (item.get("sanitized") or "").strip()
        candidate = sanitized or normalized
        if not candidate or "@" not in candidate:
            continue
        span = item.get("span")
        span_tuple: Tuple[int, int] | None
        if isinstance(span, (list, tuple)) and len(span) == 2:
            try:
                span_tuple = int(span[0]), int(span[1])
            except Exception:  # pragma: no cover - defensive
                span_tuple = None
        else:
            span_tuple = None
        snippet = slice_with_context(span_tuple)
        local, _, domain = candidate.partition("@")
        info = classify_email_role(local, domain, context_text=snippet)
        item["role_class"] = info.get("class")
        item["role_score"] = info.get("score")
        item["role_reason"] = info.get("reason")
        key = sanitized if sanitized else candidate
        per_address_infos.setdefault(key, []).append(info)
        if key not in snippets:
            snippets[key] = snippet
        if key not in contexts:
            contexts[key] = guess_context(snippet)

    raw_text_lower = raw_text.lower()
    for addr in unique:
        if addr not in snippets:
            index = raw_text_lower.find(addr.lower())
            if index >= 0:
                left = max(0, index - window)
                right = min(len(raw_text), index + len(addr) + window)
                snippets[addr] = raw_text[left:right]
            else:
                snippets[addr] = raw_text
        contexts.setdefault(addr, guess_context(snippets.get(addr, raw_text)))

    role_stats: Dict[str, int] = {"personal": 0, "role": 0, "unknown": 0}
    classified: Dict[str, Dict[str, object]] = {}
    filtered: List[str] = []
    role_filtered = 0

    for addr in unique:
        if "@" not in addr:
            continue
        infos = per_address_infos.get(addr, [])
        if not infos:
            local, _, domain = addr.partition("@")
            infos = [
                classify_email_role(local, domain, context_text=snippets.get(addr, raw_text))
            ]
        roles = [info for info in infos if str(info.get("class")) == "role"]
        if roles:
            chosen = min(roles, key=lambda info: float(info.get("score", 0.0)))
        else:
            chosen = max(infos, key=lambda info: float(info.get("score", 0.5)))
        info_class = str(chosen.get("class") or "unknown")
        if info_class not in role_stats:
            role_stats[info_class] = 0
        role_stats[info_class] += 1
        classified[addr] = dict(chosen)
        if PERSONAL_ONLY and info_class == "role":
            role_filtered += 1
            continue
        filtered.append(addr)

    # [EBOT-PARSER-FIO-003] FIO matching
    names = extract_names(raw_text)
    fio_scores: Dict[str, float] = {}
    for addr in unique:
        if "@" not in addr:
            fio_scores[addr] = 0.0
            continue
        local = addr.split("@", 1)[0]
        fio_scores[addr] = max([fio_match_score(local, fio) for fio in names] or [0.0])

    meta["role_stats"] = role_stats
    meta["classified"] = classified
    meta["source_context"] = contexts
    meta["fio_scores"] = fio_scores
    meta["role_filter_applied"] = PERSONAL_ONLY
    meta["role_filtered"] = role_filtered

    stats = {
        "found_raw": len(items),
        "after_deobfuscation": deobf_count,
        "normalized_confusables": conf_count,
        "final_unique": len(filtered),
        "dropped": dropped,
        "deobfuscated_count": deobf_count,
        "confusables_fixed": conf_count,
        "personal": role_stats.get("personal", 0),
        "role": role_stats.get("role", 0),
        "unknown": role_stats.get("unknown", 0),
        "role_filtered": role_filtered,
        "personal_only": int(PERSONAL_ONLY),
        "classified": classified,
        "contexts_tagged": len(contexts),
        "has_fio": 1 if names else 0,
    }

    return filtered, stats


def run_pipeline_on_text(text: str) -> Tuple[List[str], List[Tuple[str, str]]]:
    """Convenience helper returning final e-mails and dropped candidates."""

    cleaned, meta = parse_emails_unified(text or "", return_meta=True)
    final = dedupe_with_variants(cleaned)
    if PERSONAL_ONLY:
        filtered_final: List[str] = []
        for addr in final:
            if "@" not in addr:
                continue
            local, _, domain = addr.partition("@")
            info = classify_email_role(local, domain, context_text=text or "")
            if str(info.get("class")) == "role":
                continue
            filtered_final.append(addr)
        final = filtered_final
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

