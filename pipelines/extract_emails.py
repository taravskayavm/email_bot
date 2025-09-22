from __future__ import annotations

import asyncio
import os
import time
from typing import Callable, Dict, List, Tuple

import httpx

from emailbot import config as C
from crawler.web_crawler import Crawler
from utils.charset_helper import best_effort_decode

from utils.email_clean import (
    dedupe_with_variants,
    finalize_email,
    parse_emails_unified,
    drop_leading_char_twins,
)
from utils.dedup import canonical
from utils.text_normalize import normalize_text
from utils.email_role import classify_email_role
from utils.name_match import fio_candidates, fio_match_score

from utils.tld_utils import is_allowed_domain, is_foreign_domain

PERSONAL_ONLY = os.getenv("EMAIL_ROLE_PERSONAL_ONLY", "1") == "1"
FIO_PERSONAL_THRESHOLD = 0.9


def extract_emails_pipeline(text: str) -> Tuple[List[str], Dict[str, int]]:
    """High-level pipeline that applies deobfuscation, normalization and dedupe."""

    source_text = text or ""
    raw_text = normalize_text(source_text)
    cleaned, meta_in = parse_emails_unified(raw_text, return_meta=True)
    meta = dict(meta_in)
    items = meta.get("items", [])
    suspects = sorted(set(meta.get("suspects") or []))

    allowed_candidates: List[str] = []
    rejected: List[dict] = []
    foreign_filtered_pre = 0

    for item in items:
        normalized = (item.get("normalized") or "").strip()
        if "@" not in normalized:
            continue
        sanitized_value = (item.get("sanitized") or "").strip()
        reason_value = item.get("reason")
        local, _, domain = normalized.partition("@")
        email_final, finalize_reason, finalize_stage = finalize_email(
            local,
            domain,
            raw_text=raw_text,
            span=item.get("span"),
            sanitized=sanitized_value,
            sanitize_reason=reason_value if isinstance(reason_value, str) else None,
        )
        if finalize_reason:
            item["reason"] = finalize_reason
            item["stage"] = finalize_stage
            item["sanitized"] = ""
            rejected.append(dict(item))
            continue
        if not email_final:
            item["sanitized"] = ""
            if reason_value:
                item.setdefault("stage", "sanitize")
                rejected.append(dict(item))
            continue
        final_domain = email_final.rsplit("@", 1)[-1]
        if not is_allowed_domain(final_domain):
            item["reason"] = "tld-not-allowed"
            item["stage"] = "finalize"
            item["sanitized"] = ""
            rejected.append(dict(item))
            foreign_filtered_pre += 1
            continue
        item["sanitized"] = email_final
        allowed_candidates.append(email_final)

    # EB-REQUIRE-CONFIRM-SUSPECTS: отделяем «подозрительные» и (опционально)
    # не включаем их в отправку без явного подтверждения.
    REQUIRE_CONFIRM = os.getenv("SUSPECTS_REQUIRE_CONFIRM", "1") == "1"
    send_candidates = [candidate for candidate in allowed_candidates if candidate]
    if send_candidates:
        dedup_canonical: dict[str, str] = {}
        for candidate in send_candidates:
            key = canonical(candidate)
            if key not in dedup_canonical:
                dedup_canonical[key] = candidate
        send_candidates = list(dedup_canonical.values())
    if REQUIRE_CONFIRM:
        blocked = set(suspects)

        def _ascii_local_ok(addr: str) -> bool:
            local = addr.split("@", 1)[0]
            return bool(local) and all(0x21 <= ord(ch) <= 0x7E for ch in local)

        send_candidates = [
            candidate
            for candidate in send_candidates
            if candidate not in blocked and _ascii_local_ok(candidate)
        ]

    send_candidates = drop_leading_char_twins(send_candidates)
    unique = dedupe_with_variants(send_candidates)
    meta["items_rejected"] = rejected
    meta["emails_suspects"] = suspects
    meta["suspicious_count"] = len(suspects)
    meta["dedup_len"] = len(unique)
    fio_pairs = fio_candidates(source_text)

    def _merge_reason(reason: str | None, extra: str) -> str:
        parts = [
            part
            for part in str(reason or "").split(",")
            if part and part != "baseline"
        ]
        if extra and extra not in parts:
            parts.append(extra)
        return ",".join(parts) if parts else "baseline"

    def _apply_fio_boost(info: Dict[str, object], score: float) -> None:
        info["fio_score"] = round(float(score or 0.0), 3)
        if score >= FIO_PERSONAL_THRESHOLD and str(info.get("class")) == "unknown":
            info["class"] = "personal"
            info["score"] = max(float(info.get("score", 0.5)), 0.85)
            info["reason"] = _merge_reason(info.get("reason"), "fio-match")

    items = meta.get("items", [])
    deobf_count = meta.get("deobfuscated_count", 0)
    conf_count = meta.get("confusables_fixed", 0)
    foreign = 0
    for item in items:
        normalized = (item.get("normalized") or "").strip()
        if "@" not in normalized:
            continue
        domain = normalized.rsplit("@", 1)[1]
        if is_foreign_domain(domain):
            foreign += 1
    dropped = sum(1 for item in items if not item.get("sanitized"))
    meta["foreign_domains"] = foreign

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
        fio_score = fio_match_score(local, raw_text, candidates=fio_pairs)
        _apply_fio_boost(info, fio_score)
        item["role_class"] = info.get("class")
        item["role_score"] = info.get("score")
        item["role_reason"] = info.get("reason")
        item["fio_score"] = fio_score
        item["fio_match"] = fio_score
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
    drop_reasons: Dict[str, str] = {}
    foreign_filtered = foreign_filtered_pre
    role_filtered = 0
    fio_scores: Dict[str, float] = {}

    for addr in unique:
        if "@" not in addr:
            fio_scores[addr] = 0.0
            continue

        infos = per_address_infos.get(addr, [])
        local, _, domain = addr.partition("@")
        if not infos:
            context = snippets.get(addr, raw_text)
            info = classify_email_role(local, domain, context_text=context)
            score = fio_match_score(local, source_text, candidates=fio_pairs)
            _apply_fio_boost(info, score)
            infos = [info]
        else:
            missing = [info for info in infos if "fio_score" not in info]
            if missing:
                score = fio_match_score(local, source_text, candidates=fio_pairs)
                for info in missing:
                    _apply_fio_boost(info, score)

        roles = [info for info in infos if str(info.get("class")) == "role"]
        if roles:
            chosen = min(roles, key=lambda info: float(info.get("score", 0.0)))
        else:
            chosen = max(infos, key=lambda info: float(info.get("score", 0.5)))

        info_class = str(chosen.get("class") or "unknown")
        if info_class not in role_stats:
            role_stats[info_class] = 0
        role_stats[info_class] += 1

        fio_scores[addr] = max(float(info.get("fio_score", 0.0)) for info in infos)
        classified[addr] = dict(chosen)

        if is_foreign_domain(domain):
            foreign_filtered += 1
            drop_reasons[addr] = "tld-not-allowed"
            continue
        if PERSONAL_ONLY and info_class == "role":
            role_filtered += 1
            drop_reasons[addr] = "role-like"
            continue
        filtered.append(addr)

    meta["role_stats"] = role_stats
    meta["classified"] = classified
    meta["source_context"] = contexts
    meta_in["source_context"] = contexts
    meta["fio_scores"] = fio_scores
    meta["role_filter_applied"] = PERSONAL_ONLY
    meta["role_filtered"] = role_filtered

    filtered_set = set(filtered)
    dropped_candidates: Dict[str, str] = {}
    for item in items:
        normalized = (item.get("normalized") or "").strip()
        sanitized = (item.get("sanitized") or "").strip()
        candidate = sanitized or normalized
        if not candidate:
            continue
        if sanitized and sanitized in filtered_set:
            continue
        reason = ""
        if sanitized and sanitized in drop_reasons:
            reason = drop_reasons[sanitized]
        elif normalized and normalized in drop_reasons:
            reason = drop_reasons[normalized]
        elif not sanitized:
            reason = str(item.get("reason") or "invalid")
        if reason:
            dropped_candidates.setdefault(candidate, reason)

    meta["dropped_candidates"] = sorted(dropped_candidates.items())

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
        "has_fio": 1 if fio_pairs else 0,
        "foreign_domains": foreign,
        "foreign_filtered": foreign_filtered,
        "drop_reasons": drop_reasons,
        "dropped_candidates": sorted(dropped_candidates.items()),
        "items": items,
        "items_rejected": rejected,
        "suspicious_count": len(suspects),
    }

    return filtered, stats


def run_pipeline_on_text(text: str) -> Tuple[List[str], List[Tuple[str, str]]]:
    """Convenience helper returning final e-mails and dropped candidates."""

    cleaned, meta = parse_emails_unified(text or "", return_meta=True)
    final = dedupe_with_variants(cleaned)
    filtered_final: List[str] = []
    drop_reasons: Dict[str, str] = {}
    for addr in final:
        if "@" not in addr:
            continue
        local, _, domain = addr.partition("@")
        if is_foreign_domain(domain):
            drop_reasons[addr] = "tld-not-allowed"
            continue
        info = classify_email_role(local, domain, context_text=text or "")
        if PERSONAL_ONLY and str(info.get("class")) == "role":
            drop_reasons[addr] = "role-like"
            continue
        filtered_final.append(addr)
    final = filtered_final

    dropped: List[Tuple[str, str]] = []
    final_set = set(final)
    for item in meta.get("items", []):
        normalized = (item.get("normalized") or "").strip()
        sanitized = (item.get("sanitized") or "").strip()
        candidate = sanitized or normalized or str(item.get("raw") or "")
        if not candidate:
            continue
        if sanitized and sanitized in final_set:
            continue
        if sanitized and sanitized in drop_reasons:
            dropped.append((sanitized, drop_reasons[sanitized]))
            continue
        if normalized and normalized in drop_reasons:
            dropped.append((normalized, drop_reasons[normalized]))
            continue
        if not sanitized:
            reason = str(item.get("reason") or "invalid")
            dropped.append((candidate, reason))
    return final, dropped


def _http_get_text(url: str, *, timeout: float = 20.0) -> str:
    """Fetch a single URL synchronously and decode using charset-normalizer."""

    headers = {"User-Agent": C.CRAWL_USER_AGENT}
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=timeout,
            headers=headers,
            http2=True,
        ) as client:
            response = client.get(url)
            content_type = str(response.headers.get("content-type", "")).lower()
            if content_type and not any(
                hint in content_type for hint in ("text", "html", "xml", "json")
            ):
                return ""
            return best_effort_decode(response.content)
    except Exception:
        return ""


async def extract_from_url_async(
    url: str,
    *,
    deep: bool = True,
    progress_cb: Callable[[int, str], None] | None = None,
) -> tuple[list[str], dict]:
    """Extract e-mail addresses from ``url`` asynchronously.

    When ``deep`` is ``True`` the crawler walks the site breadth-first (respecting
    robots.txt and staying on the same domain if configured) and aggregates all
    discovered HTML pages. ``progress_cb`` is invoked with ``(pages, page_url)``
    to report crawling progress; it is throttled internally to avoid flooding.
    """

    if not deep:
        html = _http_get_text(url)
        emails, meta = extract_emails_pipeline(html or "")
        stats = dict(meta) if isinstance(meta, dict) else {}
        stats["pages"] = 1 if html else 0
        stats["unique"] = len(emails)
        stats["page_urls"] = [url] if html else []
        return emails, stats

    pages: list[tuple[str, str]] = []
    last_notify = 0.0

    def _on_page(pages_scanned: int, page_url: str) -> None:
        nonlocal last_notify
        if not progress_cb:
            return
        now = time.time()
        if now - last_notify < 0.7:
            return
        last_notify = now
        try:
            progress_cb(pages_scanned, page_url)
        except Exception:
            pass

    crawler = Crawler(url, on_page=_on_page)
    try:
        async for page_url, text in crawler.crawl():
            pages.append((page_url, text))
    finally:
        await crawler.close()

    combined_parts: list[str] = []
    for page_url, text in pages:
        marker = f"<!-- {page_url} -->"
        combined_parts.append(f"{marker}\n{text}")
    combined_text = "\n\n".join(combined_parts)

    if combined_text:
        emails_raw, meta = extract_emails_pipeline(combined_text)
    else:
        emails_raw, meta = [], {}

    emails = list(dict.fromkeys(emails_raw))
    stats = dict(meta) if isinstance(meta, dict) else {}
    stats["pages"] = crawler.pages_scanned
    stats["unique"] = len(emails)
    stats["page_urls"] = [page_url for page_url, _ in pages]
    return emails, stats


def extract_from_url(url: str, *, deep: bool = True) -> list[str]:
    """Synchronous wrapper for :func:`extract_from_url_async`."""

    emails, _ = asyncio.run(extract_from_url_async(url, deep=deep))
    return emails


__all__ = [
    "extract_emails_pipeline",
    "run_pipeline_on_text",
    "extract_from_url_async",
    "extract_from_url",
]

