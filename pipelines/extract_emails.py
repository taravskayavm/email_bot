from __future__ import annotations

import asyncio
import os
import time
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import httpx

from emailbot import config as C
from emailbot.run_control import should_stop
from crawler.web_crawler import Crawler
from utils.charset_helper import best_effort_decode

ProgressCB = Optional[Callable[[int, str], None]]

from utils.email_clean import (
    dedupe_with_variants,
    finalize_email,
    parse_emails_unified,
    drop_leading_char_twins,
    preclean_obfuscations,
)
from utils.dedup import canonical
from utils.domain_typos import autocorrect_domain
from utils.dns_check import domain_has_mx
from utils.email_norm import sanitize_for_send
from utils.text_normalize import normalize_text
from utils.email_role import classify_email_role
from utils.name_match import fio_candidates, fio_match_score

from utils.tld_utils import is_allowed_domain, is_foreign_domain

PERSONAL_ONLY = os.getenv("EMAIL_ROLE_PERSONAL_ONLY", "1") == "1"


def _env_float(name: str, default: float) -> float:
    try:
        return float((os.getenv(name, "") or "").strip())
    except Exception:
        return default


FIO_PERSONAL_THRESHOLD = _env_float("EMAIL_ROLE_PERSONAL_THRESHOLD", 0.9)


def extract_emails_pipeline(text: str) -> Tuple[List[str], Dict[str, int]]:
    """High-level pipeline that applies deobfuscation, normalization and dedupe."""

    source_text = text or ""
    source_text = preclean_obfuscations(source_text)
    raw_text = normalize_text(source_text)
    cleaned, meta_in = parse_emails_unified(raw_text, return_meta=True)
    meta = dict(meta_in)
    items = meta.get("items", [])
    suspects = sorted(set(meta.get("suspects") or []))

    allowed_candidates: List[str] = []
    rejected: List[dict] = []
    foreign_filtered_pre = 0
    role_rejected_early = 0
    aborted = False

    for item in items:
        if should_stop():
            aborted = True
            meta["aborted"] = True
            break
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
        if PERSONAL_ONLY:
            local_candidate = email_final.split("@", 1)[0]
            info = classify_email_role(local_candidate, final_domain)
            if str(info.get("class")) == "role":
                item["reason"] = "role-address"
                item["stage"] = "classify"
                item["sanitized"] = ""
                rejected.append(dict(item))
                role_rejected_early += 1
                continue
        final_for_send = sanitize_for_send(email_final)
        if not final_for_send:
            item["reason"] = "sanitize-send"
            item["stage"] = "send-normalize"
            item["sanitized"] = ""
            rejected.append(dict(item))
            continue
        if os.getenv("AUTOCORRECT_COMMON_DOMAINS", "1") == "1":
            fixed, changed, typo_reason = autocorrect_domain(final_for_send)
            if changed:
                final_for_send = fixed
                meta["typo_fixes"] = int(meta.get("typo_fixes", 0) or 0) + 1
                typo_list = list(meta.get("typo_list") or [])
                typo_list.append(typo_reason)
                meta["typo_list"] = typo_list
        domain_for_check = final_for_send.split("@", 1)[-1]
        if (
            os.getenv("MX_CHECK_BEFORE_SEND", "1") == "1"
            and not domain_has_mx(domain_for_check)
        ):
            rejected_item = dict(item)
            rejected_item["reason"] = "no-mx"
            rejected_item["stage"] = "precheck"
            rejected_item["sanitized"] = ""
            rejected.append(rejected_item)
            meta["mx_missing"] = int(meta.get("mx_missing", 0) or 0) + 1
            continue
        item["sanitized"] = final_for_send
        allowed_candidates.append(final_for_send)

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
    meta["role_rejected"] = role_rejected_early
    try:
        meta_in["role_rejected"] = role_rejected_early
    except Exception:
        pass
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
    if not aborted:
        for item in items:
            if should_stop():
                aborted = True
                meta["aborted"] = True
                break
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
        if aborted or should_stop():
            aborted = True
            meta["aborted"] = True
            break
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
        if aborted or should_stop():
            aborted = True
            meta["aborted"] = True
            break
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
    default_filtered = list(unique)
    filtered: List[str] = []
    drop_reasons: Dict[str, str] = {}
    foreign_filtered = foreign_filtered_pre
    role_filtered = 0
    fio_scores: Dict[str, float] = {}

    for addr in unique:
        if aborted or should_stop():
            aborted = True
            meta["aborted"] = True
            break
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

    if meta.get("aborted") and not filtered:
        filtered = default_filtered

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
        if aborted or should_stop():
            aborted = True
            meta["aborted"] = True
            break
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
        "role_rejected": role_rejected_early + role_filtered,
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
        "aborted": bool(meta.get("aborted", False)),
    }

    return filtered, stats


def run_pipeline_on_text(text: str) -> Tuple[List[str], List[Tuple[str, str]]]:
    """Convenience helper returning final e-mails and dropped candidates."""

    cleaned, meta = parse_emails_unified(text or "", return_meta=True)
    final = dedupe_with_variants(cleaned)
    filtered_final: List[str] = []
    drop_reasons: Dict[str, str] = {}
    for addr in final:
        if should_stop():
            break
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
        if should_stop():
            break
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
    timeout_conf = httpx.Timeout(connect=10.0, read=timeout, write=timeout, pool=10.0)
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    with httpx.Client(
        follow_redirects=True,
        timeout=timeout_conf,
        headers=headers,
        http2=C.CRAWL_HTTP2,
        limits=limits,
    ) as client:
        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                response = client.get(url)
                response.raise_for_status()
                content_type = str(response.headers.get("content-type", "")).lower()
                if content_type and not any(
                    hint in content_type for hint in ("text", "html", "xml", "json")
                ):
                    return ""
                return best_effort_decode(response.content)
            except httpx.ReadTimeout as exc:
                last_exc = exc
            except Exception as exc:
                last_exc = exc
                if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
                    # HTTP errors should propagate after retries.
                    pass
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
                continue
            break
        if last_exc:
            raise last_exc
        return ""


async def extract_from_url_async(
    url: str,
    *,
    deep: bool = True,
    progress_cb: ProgressCB = None,
    path_prefixes: Optional[Sequence[str]] = None,
    max_pages: int | None = None,
    max_depth: int | None = None,
) -> tuple[list[str], dict]:
    """Extract e-mail addresses from ``url`` asynchronously.

    When ``deep`` is ``True`` the crawler walks the site breadth-first (respecting
    robots.txt and staying on the same domain if configured) and aggregates all
    discovered HTML pages. ``progress_cb`` is invoked with ``(pages, page_url)``
    to report crawling progress; it is throttled internally to avoid flooding.
    ``path_prefixes`` (if provided) limits the deep crawl to URLs whose path
    starts with one of the prefixes. ``max_pages`` and ``max_depth`` override
    the default crawler limits when provided.
    """

    if os.getenv("CRAWLER_DISABLED", "0") == "1":
        deep = False

    prefixes_list: list[str] = []
    if path_prefixes:
        seen: list[str] = []
        for raw in path_prefixes:
            if not isinstance(raw, str):
                continue
            cleaned = raw.strip()
            if not cleaned:
                continue
            if cleaned not in seen:
                seen.append(cleaned)
        prefixes_list = seen

    if should_stop():
        stats: dict[str, object] = {
            "pages": 0,
            "unique": 0,
            "page_urls": [],
            "last_url": url,
            "aborted": True,
        }
        if prefixes_list:
            stats["path_prefixes"] = list(prefixes_list)
        return [], stats

    if not deep:
        if progress_cb:
            try:
                progress_cb(1, url)
            except Exception:
                pass
        try:
            html = _http_get_text(url)
        except httpx.HTTPError as exc:
            raise RuntimeError(
                f"HTTP ошибка при загрузке {url}: {exc.__class__.__name__}"
            ) from exc
        except Exception as exc:
            raise RuntimeError(
                f"Не удалось загрузить {url}: {exc.__class__.__name__}"
            ) from exc
        if should_stop():
            stats = {
                "pages": 0,
                "unique": 0,
                "page_urls": [],
                "last_url": url,
                "aborted": True,
            }
            if prefixes_list:
                stats["path_prefixes"] = list(prefixes_list)
            return [], stats
        emails, meta = extract_emails_pipeline(html or "")
        stats = dict(meta) if isinstance(meta, dict) else {}
        stats["pages"] = 1 if html else 0
        stats["unique"] = len(emails)
        stats["page_urls"] = [url] if html else []
        stats["last_url"] = url
        if meta and isinstance(meta, dict) and meta.get("aborted"):
            stats["aborted"] = True
        if prefixes_list:
            stats["path_prefixes"] = list(prefixes_list)
        return emails, stats

    pages: list[tuple[str, str]] = []
    last_seen = url
    aborted = False

    def _on_page(pages_scanned: int, page_url: str) -> None:
        nonlocal last_seen
        if page_url:
            last_seen = page_url
        if not progress_cb:
            return
        try:
            progress_cb(pages_scanned, page_url)
        except Exception:
            pass

    crawler = Crawler(
        url,
        max_pages=max_pages,
        max_depth=max_depth,
        on_page=_on_page,
        path_prefixes=prefixes_list,
        stop_cb=should_stop,
    )
    try:
        async for page_url, text in crawler.crawl():
            if should_stop():
                aborted = True
                break
            pages.append((page_url, text))
            if should_stop():
                aborted = True
                break
    except httpx.HTTPError as exc:
        raise RuntimeError(
            f"HTTP ошибка при загрузке {url}: {exc.__class__.__name__}"
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            f"Не удалось загрузить {url}: {exc.__class__.__name__}"
        ) from exc
    finally:
        await crawler.close()

    aborted = aborted or getattr(crawler, "stopped", False)

    if not pages and not aborted:
        last_error = getattr(crawler, "last_error", None)
        if last_error is not None:
            if isinstance(last_error, httpx.HTTPError):
                raise RuntimeError(
                    f"HTTP ошибка при загрузке {url}: {last_error.__class__.__name__}"
                ) from last_error
            raise RuntimeError(
                f"Не удалось загрузить {url}: {last_error.__class__.__name__}"
            ) from last_error

    combined_parts: list[str] = []
    for page_url, text in pages:
        if should_stop():
            aborted = True
            break
        marker = f"<!-- {page_url} -->"
        combined_parts.append(f"{marker}\n{text}")
    combined_text = "\n\n".join(combined_parts)

    if aborted:
        emails_raw, meta = [], {}
    elif combined_text:
        emails_raw, meta = extract_emails_pipeline(combined_text)
    else:
        emails_raw, meta = [], {}
    if isinstance(meta, dict) and meta.get("aborted"):
        aborted = True

    emails = list(dict.fromkeys(emails_raw))
    stats = dict(meta) if isinstance(meta, dict) else {}
    stats["pages"] = crawler.pages_scanned
    stats["unique"] = len(emails)
    stats["page_urls"] = [page_url for page_url, _ in pages]
    stats["last_url"] = last_seen
    stats["aborted"] = bool(stats.get("aborted") or aborted)
    stats["max_pages"] = crawler.max_pages
    stats["max_depth"] = crawler.max_depth
    if prefixes_list:
        stats["path_prefixes"] = list(prefixes_list)
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

