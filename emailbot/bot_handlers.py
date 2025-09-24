"""Telegram bot handlers."""

from __future__ import annotations

# isort:skip_file
import asyncio
import csv
import functools
import imaplib
import inspect
import json
import logging
import os
import random
import re
import secrets
import smtplib
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import aiohttp
from telegram import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import ContextTypes

from bot.keyboards import build_templates_kb, build_parse_mode_kb
from services.templates import get_template, get_template_label
from emailbot import config as C
from emailbot.notify import notify
from .diag import build_diag_text, env_snapshot, imap_ping, smtp_ping, smtp_settings

from utils.email_clean import (
    canonicalize_email,
    dedupe_keep_original,
    drop_leading_char_twins,
    parse_emails_unified,
)
from pipelines.extract_emails import (
    extract_emails_pipeline,
    extract_from_url_async,
)
from utils import rules
from utils.send_stats import summarize_today, summarize_week, current_tz_label
from utils.send_stats import _stats_path  # только для отображения пути
from utils.bounce import sync_bounces
from utils.tld_utils import is_allowed_domain as _is_allowed_domain, is_foreign_domain

STATS_PATH = str(_stats_path())
_FALLBACK_EMAIL_RX = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
URL_RX = re.compile(r"https?://\S+", re.IGNORECASE)

from . import extraction as _extraction
from . import extraction_url as _extraction_url
from . import mass_state, messaging
from . import messaging_utils as mu
from . import settings
from .edit_service import apply_edits
from .extraction import normalize_email, smart_extract_emails
from .reporting import build_mass_report_text, log_mass_filter_digest
from .settings_store import DEFAULTS
from .services.cooldown import should_skip_by_cooldown
from .services.cooldown import COOLDOWN_DAYS


# --- EB-2025-09-23-17: user-friendly preview after extraction -----------------

def _format_preview_text(
    found: Iterable[str] | None,
    allowed: Iterable[str] | None,
    rejected: Iterable[object] | None,
    stats: Mapping[str, Any] | None,
) -> str:
    found_unique = {
        str(entry).strip()
        for entry in (found or [])
        if isinstance(entry, str) and "@" in entry
    }
    allowed_list: list[str] = []
    if allowed:
        allowed_list = list(
            dict.fromkeys(
                [
                    str(entry).strip()
                    for entry in allowed
                    if isinstance(entry, str) and "@" in entry
                ]
            )
        )
    _ = rejected  # retained for future use / signature compatibility
    stats_map: Mapping[str, Any] = stats or {}
    suspicious = int(stats_map.get("suspicious_count", 0) or 0)
    role_rejected = int(stats_map.get("role_rejected", 0) or 0)
    foreign = int(stats_map.get("foreign_domains", 0) or 0)
    sample = "\n".join(f"• {addr}" for addr in allowed_list[:10]) or "—"
    lines = [
        "✅ Предварительный результат:",
        f"• найдено адресов: {len(found_unique)}",
        f"• к отправке (после фильтров): {len(allowed_list)}",
        f"• отсечено подозрительных: {suspicious}",
        f"• рольовых (info/support и т.п.): {role_rejected}",
        f"• иностранных доменов: {foreign}",
        f"Примеры:\n{sample}",
    ]
    return "\n".join(lines)


# --- Новое состояние для ручного ввода исправлений ---
EDIT_SUSPECTS_INPUT = 9301


def _preclean_text_for_emails(text: str) -> str:
    return text


def apply_numeric_truncation_removal(allowed):
    return allowed, []


def _meta_candidate(info, *, prefer_sanitized: bool = False) -> str:
    """Extract a representative candidate string from parser meta info."""

    if not isinstance(info, dict):
        return ""
    keys = ["normalized", "raw"]
    if prefer_sanitized:
        keys = ["sanitized", *keys]
    for key in keys:
        value = info.get(key)
        if value:
            return str(value).strip()
    return ""


def _collect_preview_found(stats: Mapping[str, Any] | None) -> set[str]:
    found: set[str] = set()
    if not isinstance(stats, Mapping):
        return found

    for key in ("items", "items_rejected"):
        entries = stats.get(key)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            candidate = _meta_candidate(entry, prefer_sanitized=True) or _meta_candidate(entry)
            candidate = candidate.strip()
            if candidate and "@" in candidate:
                found.add(candidate)

    suspects = stats.get("emails_suspects")
    if isinstance(suspects, (list, tuple, set)):
        for entry in suspects:
            try:
                candidate = str(entry).strip()
            except Exception:
                continue
            if candidate and "@" in candidate:
                found.add(candidate)

    return found


def _ingest_meta_to(
    loose_target: Set[str], suspicious_target: Dict[str, str], stats_obj
) -> None:
    """Populate loose candidates and suspicious reasons from meta stats."""

    if not isinstance(stats_obj, dict):
        return
    items = stats_obj.get("items")
    if isinstance(items, list):
        for item in items:
            candidate = _meta_candidate(item)
            if candidate:
                loose_target.add(candidate)
    rejected = stats_obj.get("items_rejected", [])
    if isinstance(rejected, list):
        for info in rejected:
            candidate = _meta_candidate(info)
            if candidate:
                loose_target.add(candidate)
            display_candidate = _meta_candidate(info, prefer_sanitized=True)
            if not display_candidate:
                continue
            reason = str(info.get("reason") or "invalid").strip() or "invalid"
            suspicious_target.setdefault(display_candidate, reason)
    # EB-REQUIRE-CONFIRM-SUSPECTS: поддержка meta['emails_suspects'] из pipeline
    suspects = stats_obj.get("emails_suspects") if isinstance(stats_obj, dict) else None
    if isinstance(suspects, (list, tuple, set)):
        for entry in suspects:
            try:
                addr = str(entry).strip()
            except Exception:
                continue
            if addr:
                suspicious_target.setdefault(addr, "suspect")


async def async_extract_emails_from_url(
    url: str,
    session,
    chat_id=None,
    batch_id: str | None = None,
    *,
    deep: bool = True,
    progress_cb: Callable[[int, str], None] | None = None,
    path_prefixes: Sequence[str] | None = None,
):
    _ = _extraction.extract_any  # keep reference for tests
    emails_list, stats_raw = await extract_from_url_async(
        url,
        deep=deep,
        progress_cb=progress_cb,
        path_prefixes=path_prefixes,
    )
    meta_dict = dict(stats_raw) if isinstance(stats_raw, dict) else {}
    emails = {str(addr).strip() for addr in emails_list if addr}
    loose_candidates: Set[str] = set()
    _ingest_meta_to(loose_candidates, {}, meta_dict)
    all_found = {addr for addr in emails | loose_candidates if addr}
    foreign = {
        addr
        for addr in all_found
        if "@" in addr and not _is_allowed_domain(addr.rsplit("@", 1)[-1])
    }
    stats: dict = dict(meta_dict)
    logger.info(
        "extraction complete",
        extra={
            "event": "extract",
            "source": url,
            "count": len(emails),
            "pages": stats.get("pages", 0),
        },
    )
    return url, emails, foreign, [], stats


def collapse_footnote_variants(emails):
    return emails


def collect_repairs_from_files(files):
    return []


async def extract_emails_from_zip(path: str, *_, **__):
    emails, stats = await asyncio.to_thread(_extraction.extract_any, path)
    emails = set(e.lower().strip() for e in emails)
    extracted_files = [path]
    logger.info(
        "extraction complete",
        extra={"event": "extract", "source": path, "count": len(emails)},
    )
    return emails, extracted_files, set(emails), stats


def extract_emails_loose(text):
    return set(smart_extract_emails(text))


def extract_from_uploaded_file(path: str):
    emails, stats = _extraction.extract_any(path)
    emails = set(e.lower().strip() for e in emails)
    logger.info(
        "extraction complete",
        extra={"event": "extract", "source": path, "count": len(emails)},
    )
    return emails, set(emails), stats


def is_allowed_tld(email_addr: str) -> bool:
    addr = (email_addr or "").strip().lower()
    if "@" not in addr:
        return False
    domain = addr.rsplit("@", 1)[-1]
    return _is_allowed_domain(domain)


def sample_preview(items, k: int):
    lst = list(dict.fromkeys(items))
    if len(lst) <= k:
        return lst
    rng = random.SystemRandom()    # не влияет на глобальное состояние
    return rng.sample(lst, k)


def _normalize_email_lower(value: Any) -> str:
    if value is None:
        return ""
    try:
        text = str(value).strip()
    except Exception:
        return ""
    if not text:
        return ""
    return text.lower()


def _classify_emails(
    emails: Iterable[str],
    dropped: Sequence[tuple[str, str]] | None = None,
    cooldown_candidates: Iterable[str] | None = None,
) -> Dict[str, Any]:
    """Return disjoint e-mail sets for reporting."""

    dropped = dropped or []
    originals: Dict[str, str] = {}
    all_set: Set[str] = set()

    for entry in emails:
        norm = _normalize_email_lower(entry)
        if not norm:
            continue
        all_set.add(norm)
        try:
            originals.setdefault(norm, str(entry).strip())
        except Exception:
            originals.setdefault(norm, norm)

    reasons: Dict[str, str] = {}
    dropped_order: List[str] = []
    for addr, reason in dropped:
        norm = _normalize_email_lower(addr)
        if not norm:
            continue
        if norm not in reasons:
            try:
                text = "" if reason is None else str(reason)
            except Exception:
                text = ""
            reasons[norm] = text
            dropped_order.append(norm)
        try:
            originals.setdefault(norm, str(addr).strip())
        except Exception:
            originals.setdefault(norm, norm)

    foreign_set: Set[str] = set()
    for addr in all_set:
        if "@" not in addr:
            continue
        domain = addr.rsplit("@", 1)[-1]
        try:
            if is_foreign_domain(domain):
                foreign_set.add(addr)
        except Exception:
            foreign_set.add(addr)

    removed = set(reasons)
    clean_set = all_set - removed - foreign_set

    if cooldown_candidates is None:
        cooldown_set: Set[str] = set()
        for addr in clean_set:
            try:
                skip, _ = messaging._should_skip_by_history(addr)
            except Exception:
                continue
            if skip:
                cooldown_set.add(addr)
    else:
        cooldown_norm = {
            _normalize_email_lower(addr) for addr in cooldown_candidates if addr
        }
        cooldown_set = {addr for addr in clean_set if addr in cooldown_norm}

    send_set = clean_set - cooldown_set
    suspect_set = {addr for addr, reason in reasons.items() if reason == "suspect"}

    return {
        "all": all_set,
        "sus": suspect_set,
        "foreign": foreign_set,
        "clean": clean_set,
        "cool": cooldown_set,
        "send": send_set,
        "reasons": reasons,
        "original": originals,
        "dropped_order": dropped_order,
    }


from .messaging import (  # noqa: E402,F401  # isort: skip
    DOWNLOAD_DIR,
    LOG_FILE,
    MAX_EMAILS_PER_DAY,
    SendOutcome,
    add_blocked_email,
    clear_recent_sent_cache,
    dedupe_blocked_file,
    get_blocked_emails,
    get_preferred_sent_folder,
    get_sent_today,
    log_sent_email,
    send_email_with_sessions,
    sync_log_with_imap,
    was_emailed_recently,
)
from .messaging_utils import (  # noqa: E402  # isort: skip
    BOUNCE_LOG_PATH,
    add_bounce,
    is_foreign,
    is_hard_bounce,
    is_soft_bounce,
    is_suppressed,
    suppress_add,
)
from .utils import log_error  # noqa: E402
from utils.smtp_client import RobustSMTP  # noqa: E402

from . import history_service

from emailbot.handlers import (
    start,
    manual_mode,
    select_group,
    proceed_to_group,
    send_all,
    preview_go_back,
    preview_request_edit,
    preview_show_edits,
    preview_reset_edits,
    preview_refresh_choice,
)

logger = logging.getLogger(__name__)

ADMIN_IDS = {
    int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
}

PREVIEW_ALLOWED = int(os.getenv("EXAMPLES_COUNT", "10"))
PREVIEW_FOREIGN = 5

TECH_PATTERNS = [
    "noreply",
    "no-reply",
    "do-not-reply",
    "donotreply",
    "postmaster",
    "mailer-daemon",
    "abuse",
    "support",
    "admin",
    "info@",
]

# EB-REQUIRE-CONFIRM-SUSPECTS: флаг «подтверждать подозрительные перед отправкой»
SUSPECTS_REQUIRE_CONFIRM = os.getenv("SUSPECTS_REQUIRE_CONFIRM", "1") == "1"

# Универсальные эвристики «подозрительности» локала (без словарей)
_ORCID_PREFIX_RE = re.compile(r"^(?:\d{4}-){3,}\d{3,}[-\d]*", re.ASCII)


def _starts_with_long_digits(local: str, n: int = 5) -> bool:
    if not local:
        return False
    run = 0
    for ch in local:
        if ch.isdigit():
            run += 1
            if run >= n:
                return True
        else:
            break
    return False


def _starts_with_orcid_like(local: str) -> bool:
    return bool(_ORCID_PREFIX_RE.match(local or ""))


def _long_alpha_run_no_separators(local: str, min_len: int = 14) -> bool:
    if not local or len(local) < min_len:
        return False
    if not all(ch.isalpha() for ch in local):
        return False
    if any(ch in "._+-" for ch in local):
        return False
    return True


def _is_suspect_email(addr: str) -> bool:
    addr = (addr or "").strip().lower()
    if "@" not in addr:
        return False
    local = addr.split("@", 1)[0]
    return (
        _starts_with_long_digits(local)
        or _starts_with_orcid_like(local)
        or _long_alpha_run_no_separators(local)
    )


MAX_TG_MESSAGE = 4096
_PARAGRAPH_CHUNK = 3000


def _split_for_telegram(text: str) -> list[str]:
    parts: list[str] = []
    current = ""
    for block in text.split("\n\n"):
        if not block:
            candidate = current + ("\n\n" if current else "")
            if len(candidate) <= MAX_TG_MESSAGE:
                current = candidate
            else:
                if current:
                    parts.append(current)
                current = ""
            continue
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= MAX_TG_MESSAGE:
            current = candidate
            continue
        if current:
            parts.append(current)
            current = ""
        if len(block) <= MAX_TG_MESSAGE:
            current = block
            continue
        start = 0
        while start < len(block):
            chunk = block[start : start + _PARAGRAPH_CHUNK]
            parts.append(chunk)
            start += _PARAGRAPH_CHUNK
    if current:
        parts.append(current)
    return [part for part in parts if part]


async def _safe_reply_text(message, text: str, **kwargs):
    if not text:
        return
    if len(text) <= MAX_TG_MESSAGE:
        await message.reply_text(text, **kwargs)
        return
    chunks = _split_for_telegram(text)
    if not chunks:
        return
    first, *rest = chunks
    await message.reply_text(first, **kwargs)
    for part in rest:
        await message.reply_text(part)


def _drop_truncated_twins(
    emails: Sequence[str],
    state: SessionState | None = None,
    *,
    update_counter: bool = True,
) -> list[str]:
    items = list(emails)
    cleaned = drop_leading_char_twins(items)
    if state is not None and update_counter:
        removed = len(items) - len(cleaned)
        if removed:
            state.footnote_dupes = (state.footnote_dupes or 0) + removed
    return cleaned


@dataclass
class SessionState:
    all_emails: Set[str] = field(default_factory=set)
    all_files: List[str] = field(default_factory=list)
    to_send: List[str] = field(default_factory=list)
    foreign: List[str] = field(default_factory=list)
    preview_allowed_all: List[str] = field(default_factory=list)
    dropped: List[Tuple[str, str]] = field(default_factory=list)
    repairs: List[tuple[str, str]] = field(default_factory=list)
    repairs_sample: List[str] = field(default_factory=list)
    cooldown_blocked: List[str] = field(default_factory=list)
    group: Optional[str] = None  # template code
    template: Optional[str] = None
    template_label: Optional[str] = None
    footnote_dupes: int = 0


def _normalize_template_code(code: str) -> str:
    return (code or "").strip().lower()


def _template_label(info) -> str:
    if not info:
        return ""
    label = info.get("label") if isinstance(info, dict) else ""
    if not label:
        label = info.get("code") if isinstance(info, dict) else ""
    return str(label or "")


def _template_path(info) -> Path | None:
    if not info or not isinstance(info, dict):
        return None
    path = info.get("path")
    if not path:
        return None
    try:
        return Path(path)
    except Exception:
        return None


def get_template_from_map(
    context: ContextTypes.DEFAULT_TYPE,
    prefix: str,
    key: str,
) -> dict | None:
    """Return template info stored in ``context.user_data`` for the given key."""

    if not context or not hasattr(context, "user_data"):
        return None
    groups_map = context.user_data.get("groups_map")
    if not isinstance(groups_map, dict):
        return None
    prefix_map = groups_map.get(prefix)
    if not isinstance(prefix_map, dict):
        return None
    normalized = _normalize_template_code(key)
    info = prefix_map.get(normalized)
    if not isinstance(info, dict):
        return None
    result = dict(info)
    if "code" in result:
        result["code"] = str(result.get("code") or "")
    if "label" in result:
        result["label"] = str(result.get("label") or "")
    if "path" in result:
        result["path"] = str(result.get("path") or "")
    return result


FORCE_SEND_CHAT_IDS: set[int] = set()
SESSION_KEY = "state"


# === Конфиг для ручной рассылки (правило 180 дней) ===
def _manual_cfg():
    import os

    enforce = os.getenv("MANUAL_ENFORCE_180", "1") == "1"
    default_days = history_service.get_days_rule_default()
    try:
        days = int(os.getenv("MANUAL_DAYS", str(default_days)))
    except Exception:
        days = default_days
    allow_override = os.getenv("MANUAL_ALLOW_OVERRIDE", "1") == "1"
    return enforce, days, allow_override


MANUAL_OVERRIDE_PAGE_SIZE = 6


def _manual_override_clear(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in (
        "manual_override_candidates",
        "manual_override_selected",
        "manual_override_page",
        "manual_override_days",
    ):
        context.chat_data.pop(key, None)


def _manual_override_prepare(
    context: ContextTypes.DEFAULT_TYPE,
    rejected: Sequence[str],
    days: int,
) -> None:
    candidates: list[dict[str, str]] = []
    for email_addr in rejected:
        skip, reason = should_skip_by_cooldown(email_addr, days=days)
        reason_text = reason or f"cooldown<{days}d"
        candidates.append({"email": email_addr, "reason": reason_text})
    context.chat_data["manual_override_candidates"] = candidates
    context.chat_data["manual_override_selected"] = []
    context.chat_data["manual_override_page"] = 0
    context.chat_data["manual_override_days"] = days


def _manual_override_candidates(
    context: ContextTypes.DEFAULT_TYPE,
) -> list[dict[str, str]]:
    raw = context.chat_data.get("manual_override_candidates") or []
    if not isinstance(raw, list):
        return []
    candidates: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        email_addr = str(item.get("email") or "")
        if not email_addr:
            continue
        reason_text = str(item.get("reason") or "")
        candidates.append({"email": email_addr, "reason": reason_text})
    return candidates


def _manual_override_selected_set(context: ContextTypes.DEFAULT_TYPE) -> set[str]:
    stored = context.chat_data.get("manual_override_selected")
    result: set[str] = set()
    if isinstance(stored, (list, set, tuple)):
        for item in stored:
            if item:
                result.add(str(item))
    elif isinstance(stored, str):
        result.add(stored)
    return result


def _manual_override_store_selected(
    context: ContextTypes.DEFAULT_TYPE,
    selected: set[str],
) -> None:
    candidate_emails = {item["email"] for item in _manual_override_candidates(context)}
    cleaned = sorted(email for email in selected if email in candidate_emails)
    previous = _manual_override_selected_set(context)
    if previous == set(cleaned):
        return
    context.chat_data["manual_override_selected"] = cleaned


def _normalize_manual_drop_reason(reason: str) -> str:
    clean = (reason or "").strip()
    if not clean:
        return ""
    mapping = {
        "baseline": "invalid",
        "role-like-prefix": "role-like",
    }
    return mapping.get(clean, clean)


def _manual_collect_drop_reasons(
    stats: Dict[str, object] | None,
    final_emails: Sequence[str],
    truncated_removed: Sequence[str],
) -> List[Tuple[str, str]]:
    if not stats:
        return []
    dropped_pairs = stats.get("dropped_candidates") if isinstance(stats, dict) else None
    if not isinstance(dropped_pairs, list):
        dropped_pairs = []
    items = stats.get("items") if isinstance(stats, dict) else None
    lookup: Dict[str, List[dict]] = {}
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            normalized = str(item.get("normalized") or "").strip()
            sanitized = str(item.get("sanitized") or "").strip()
            candidates = [c for c in (sanitized, normalized) if c]
            if not candidates:
                raw = str(item.get("raw") or "").strip()
                if raw:
                    candidates = [raw]
            for candidate in candidates:
                lookup.setdefault(candidate, []).append(item)
    final_set = set(final_emails)
    entries: Dict[str, str] = {}
    for addr, raw_reason in dropped_pairs:
        if addr in final_set:
            continue
        reason = _normalize_manual_drop_reason(str(raw_reason or ""))
        if not reason:
            continue
        meta_list = lookup.get(addr, [])
        max_fio = 0.0
        for info in meta_list:
            try:
                score = float(info.get("fio_match") or info.get("fio_score") or 0.0)
            except Exception:
                continue
            if score > max_fio:
                max_fio = score
        if max_fio >= 1.0:
            continue
        entries.setdefault(addr, reason)
    for addr in truncated_removed:
        if addr in final_set:
            continue
        entries.setdefault(addr, "truncated-duplicate")
    if not entries:
        return []
    return sorted(entries.items(), key=lambda pair: pair[0].lower())


def _manual_override_current_page(context: ContextTypes.DEFAULT_TYPE) -> int:
    page_raw = context.chat_data.get("manual_override_page", 0)
    try:
        page = int(page_raw)
    except Exception:
        page = 0
    return max(0, page)


def _manual_override_render(
    context: ContextTypes.DEFAULT_TYPE,
    page: int,
) -> tuple[str, InlineKeyboardMarkup]:
    candidates = _manual_override_candidates(context)
    if not candidates:
        return "", InlineKeyboardMarkup([])
    per_page = max(1, MANUAL_OVERRIDE_PAGE_SIZE)
    total = len(candidates)
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    context.chat_data["manual_override_page"] = page
    start = page * per_page
    chunk = candidates[start : start + per_page]
    selected = _manual_override_selected_set(context)
    selected_in_candidates = {
        item["email"] for item in candidates if item["email"] in selected
    }
    days_raw = context.chat_data.get("manual_override_days")
    days = days_raw if isinstance(days_raw, int) and days_raw > 0 else None

    lines: list[str] = []
    if days:
        lines.append(
            f"Отфильтровано правилом {days} дней. Выберите адреса для игнорирования."
        )
    else:
        lines.append("Выберите адреса для игнорирования правила 180 дней.")
    lines.append(f"Выбрано: {len(selected_in_candidates)} из {total}.")
    if pages > 1:
        lines.append(f"Страница {page + 1}/{pages}.")
    for idx, item in enumerate(chunk, start=start):
        mark = "✅" if item["email"] in selected else "◻️"
        reason = item.get("reason") or "recent-send"
        lines.append(f"{mark} {idx + 1}) {item['email']} — {reason}")
    text = "\n".join(lines)

    buttons = [
        InlineKeyboardButton(
            f"{'✅' if item['email'] in selected else '◻️'} {idx + 1}",
            callback_data=f"manual_ignore_selected:toggle:{idx}",
        )
        for idx, item in enumerate(chunk, start=start)
    ]
    rows: list[list[InlineKeyboardButton]] = []
    for i in range(0, len(buttons), 3):
        rows.append(buttons[i : i + 3])
    rows.append(
        [
            InlineKeyboardButton(
                "Игнорировать (выбранные)",
                callback_data="manual_ignore_selected:apply",
            )
        ]
    )
    nav_row: list[InlineKeyboardButton] = []
    if pages > 1 and page > 0:
        nav_row.append(
            InlineKeyboardButton(
                "⬅️", callback_data=f"manual_ignore_selected:page:{page - 1}"
            )
        )
    nav_row.append(
        InlineKeyboardButton(
            "Очистить выбор", callback_data="manual_ignore_selected:clear"
        )
    )
    if pages > 1 and page < pages - 1:
        nav_row.append(
            InlineKeyboardButton(
                "➡️", callback_data=f"manual_ignore_selected:page:{page + 1}"
            )
        )
    rows.append(nav_row)
    rows.append(
        [InlineKeyboardButton("Закрыть", callback_data="manual_ignore_selected:close")]
    )
    return text, InlineKeyboardMarkup(rows)


async def _manual_override_show(
    query: CallbackQuery,
    context: ContextTypes.DEFAULT_TYPE,
    page: int,
    *,
    edit: bool = False,
) -> None:
    text, markup = _manual_override_render(context, page)
    if not text:
        if edit:
            await query.message.edit_text("Список пуст.")
        else:
            await _safe_reply_text(query.message, "Список пуст.")
        return
    if edit:
        await query.message.edit_text(text, reply_markup=markup)
    else:
        await _safe_reply_text(query.message, text, reply_markup=markup)


def _filter_by_180(
    emails: list[str], group: str, days: int, chat_id: int | None = None
) -> tuple[list[str], list[str]]:
    """Разделяет список на разрешённые и отклонённые по правилу N дней."""

    to_check = list(emails)
    if chat_id is not None:
        try:
            to_check = apply_edits(to_check, chat_id)
        except Exception:  # pragma: no cover - defensive fallback
            to_check = list(emails)
    try:
        allowed, rejected = history_service.filter_by_days(to_check, group, days)
    except Exception:  # pragma: no cover - defensive fallback
        # в случае ошибки проверки — перестрахуемся и разрешим
        allowed, rejected = to_check, []

    extra_recent: list[str] = []
    allowed_final: list[str] = []
    for email in allowed:
        if rules.seen_within_window(email):
            extra_recent.append(email)
        else:
            allowed_final.append(email)

    if extra_recent:
        rejected = list(rejected) + extra_recent

    return allowed_final, rejected


def init_state(context: ContextTypes.DEFAULT_TYPE) -> SessionState:
    """Initialize session state for the current chat."""
    state = SessionState()
    context.chat_data[SESSION_KEY] = state
    context.chat_data["cancel_event"] = asyncio.Event()
    return state


def get_state(context: ContextTypes.DEFAULT_TYPE) -> SessionState:
    """Return existing session state or initialize a new one."""
    return context.chat_data.get(SESSION_KEY) or init_state(context)


def enable_force_send(chat_id: int) -> None:
    """Allow this chat to bypass the daily sending limit."""

    FORCE_SEND_CHAT_IDS.add(chat_id)


def disable_force_send(chat_id: int) -> None:
    """Disable the force-send mode for the chat."""

    FORCE_SEND_CHAT_IDS.discard(chat_id)


def is_force_send(chat_id: int) -> bool:
    """Return ``True`` if the chat bypasses the daily limit."""

    return chat_id in FORCE_SEND_CHAT_IDS


def clear_all_awaiting(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Reset all awaiting flags stored in ``user_data``."""

    for key in ["awaiting_block_email", "awaiting_manual_email"]:
        context.user_data[key] = False


async def features(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show key feature flags and provide toggles for advanced settings."""

    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await _safe_reply_text(update.message, "Команда доступна только администратору.")
        return

    settings.load()

    def _env_or_default(name: str, fallback: str) -> str:
        value = os.getenv(name)
        if value is None or value == "":
            return fallback
        return value

    def _summary() -> str:
        append_default = "on" if messaging.APPEND_TO_SENT else "off"
        parts = ["⚙️ Включённые функции"]
        parts.append(
            f"• DAILY_SEND_LIMIT: {_env_or_default('DAILY_SEND_LIMIT', str(settings.DAILY_SEND_LIMIT))}"
        )
        parts.append(
            f"• COOLDOWN_DAYS: {_env_or_default('COOLDOWN_DAYS', str(COOLDOWN_DAYS))}"
        )
        parts.append(
            f"• APPEND_TO_SENT: {_env_or_default('APPEND_TO_SENT', f'default({append_default})')}"
        )
        parts.append(
            f"• OBFUSCATION_ENABLE: {_env_or_default('OBFUSCATION_ENABLE', '0')}"
        )
        parts.append(
            f"• CONFUSABLES_NORMALIZE: {_env_or_default('CONFUSABLES_NORMALIZE', '0')}"
        )
        parts.append(
            f"• STRICT_DOMAIN_VALIDATE: {_env_or_default('STRICT_DOMAIN_VALIDATE', '0')}"
        )
        parts.append(
            f"• IDNA_DOMAIN_NORMALIZE: {_env_or_default('IDNA_DOMAIN_NORMALIZE', '0')}"
        )
        parts.append(f"• INLINE_LOGO: {_env_or_default('INLINE_LOGO', '0')}")
        parts.append(
            f"• EMAIL_ROLE_PERSONAL_ONLY: {_env_or_default('EMAIL_ROLE_PERSONAL_ONLY', '1')}"
        )
        parts.append(
            f"• STRICT_OBFUSCATION: {'on' if settings.STRICT_OBFUSCATION else 'off'}"
        )
        parts.append(
            f"• PDF_LAYOUT_AWARE: {'on' if settings.PDF_LAYOUT_AWARE else 'off'}"
        )
        parts.append(f"• ENABLE_OCR: {'on' if settings.ENABLE_OCR else 'off'}")
        return "\n".join(parts)

    def _status() -> str:
        lines = []
        line = f"STRICT_OBFUSCATION={'on' if settings.STRICT_OBFUSCATION else 'off'}"
        if settings.STRICT_OBFUSCATION == DEFAULTS["STRICT_OBFUSCATION"]:
            line += " (рекомендуется)"
        lines.append(line)
        line = f"FOOTNOTE_RADIUS_PAGES={settings.FOOTNOTE_RADIUS_PAGES}"
        if settings.FOOTNOTE_RADIUS_PAGES == DEFAULTS["FOOTNOTE_RADIUS_PAGES"]:
            line += " (рекомендуется)"
        lines.append(line)
        line = f"PDF_LAYOUT_AWARE={'on' if settings.PDF_LAYOUT_AWARE else 'off'}"
        if settings.PDF_LAYOUT_AWARE == DEFAULTS["PDF_LAYOUT_AWARE"]:
            line += " (рекомендуется)"
        lines.append(line)
        line = f"ENABLE_OCR={'on' if settings.ENABLE_OCR else 'off'}"
        if settings.ENABLE_OCR == DEFAULTS["ENABLE_OCR"]:
            line += " (рекомендуется)"
        lines.append(line)
        return "\n".join(lines)

    def _keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"Обфускации: {'Строгий' if settings.STRICT_OBFUSCATION else 'Обычный'} ⏼",
                        callback_data="feat:strict:toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Сноски: радиус 0", callback_data="feat:radius:0"
                    ),
                    InlineKeyboardButton("1", callback_data="feat:radius:1"),
                    InlineKeyboardButton("2", callback_data="feat:radius:2"),
                ],
                [
                    InlineKeyboardButton(
                        f"PDF-layout {'on' if settings.PDF_LAYOUT_AWARE else 'off'} ⏼",
                        callback_data="feat:layout:toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"OCR {'on' if settings.ENABLE_OCR else 'off'} ⏼",
                        callback_data="feat:ocr:toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Сбросить к рекомендованным",
                        callback_data="feat:reset:defaults",
                    )
                ],
            ]
        )

    def _doc() -> str:
        return (
            "ℹ️ Рекомендуемые настройки: строгие обфускации — ON, радиус сносок — 1, "
            "PDF-layout — OFF, OCR — OFF."
        )

    summary = _summary()
    text = f"{summary}\n\n{_status()}\n\n{_doc()}"
    await _safe_reply_text(update.message, text, reply_markup=_keyboard())


async def features_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle feature toggle button callbacks."""

    query = update.callback_query
    if not query:
        return
    user = query.from_user
    if not user or user.id not in ADMIN_IDS:
        await query.answer()
        return

    settings.load()

    data = query.data or ""
    hint = ""
    try:
        if data == "feat:strict:toggle":
            settings.STRICT_OBFUSCATION = not settings.STRICT_OBFUSCATION
            hint = (
                "🛡️ Строгий режим включён. Парсер принимает обфускации только с явными “at/dot”. "
                "Ложные «121536@gmail.com» с чисел не появятся. На реальные адреса с @/mailto это не влияет."
                if settings.STRICT_OBFUSCATION
                else "⚠️ Строгий режим выключен. Парсер будет пытаться восстановить адреса из менее явных обфускаций. Возможен рост ложных совпадений на «число + домен»."
            )
        elif data.startswith("feat:radius:"):
            n = int(data.rsplit(":", 1)[-1])
            if n not in {0, 1, 2}:
                raise ValueError
            settings.FOOTNOTE_RADIUS_PAGES = n
            hint = f"📝 Радиус сносок: {n}. Дубликаты «урезанных» адресов будут склеиваться в пределах той же страницы и ±{n} стр. того же файла."
        elif data == "feat:layout:toggle":
            settings.PDF_LAYOUT_AWARE = not settings.PDF_LAYOUT_AWARE
            hint = (
                "📄 Учёт макета PDF включён. Надстрочные (сноски) обрабатываются точнее. Может работать медленнее на больших PDF."
                if settings.PDF_LAYOUT_AWARE
                else "📄 Учёт макета PDF выключен. Используется стандартное извлечение текста."
            )
        elif data == "feat:ocr:toggle":
            settings.ENABLE_OCR = not settings.ENABLE_OCR
            hint = (
                "🔍 OCR включён. Будем распознавать e-mail в скан-PDF. Анализ станет медленнее. Ограничения: до 10 страниц, таймаут 30 сек."
                if settings.ENABLE_OCR
                else "🔍 OCR выключен. Скан-PDF без текста пропускаются без распознавания."
            )
        elif data == "feat:reset:defaults":
            settings.STRICT_OBFUSCATION = DEFAULTS["STRICT_OBFUSCATION"]
            settings.FOOTNOTE_RADIUS_PAGES = DEFAULTS["FOOTNOTE_RADIUS_PAGES"]
            settings.PDF_LAYOUT_AWARE = DEFAULTS["PDF_LAYOUT_AWARE"]
            settings.ENABLE_OCR = DEFAULTS["ENABLE_OCR"]
            hint = "↩️ Сброшено к рекомендованным настройкам."
        else:
            raise ValueError
        settings.save()
    except Exception:
        hint = "⛔ Недопустимое значение."

    def _status() -> str:
        lines = []
        line = f"STRICT_OBFUSCATION={'on' if settings.STRICT_OBFUSCATION else 'off'}"
        if settings.STRICT_OBFUSCATION == DEFAULTS["STRICT_OBFUSCATION"]:
            line += " (рекомендуется)"
        lines.append(line)
        line = f"FOOTNOTE_RADIUS_PAGES={settings.FOOTNOTE_RADIUS_PAGES}"
        if settings.FOOTNOTE_RADIUS_PAGES == DEFAULTS["FOOTNOTE_RADIUS_PAGES"]:
            line += " (рекомендуется)"
        lines.append(line)
        line = f"PDF_LAYOUT_AWARE={'on' if settings.PDF_LAYOUT_AWARE else 'off'}"
        if settings.PDF_LAYOUT_AWARE == DEFAULTS["PDF_LAYOUT_AWARE"]:
            line += " (рекомендуется)"
        lines.append(line)
        line = f"ENABLE_OCR={'on' if settings.ENABLE_OCR else 'off'}"
        if settings.ENABLE_OCR == DEFAULTS["ENABLE_OCR"]:
            line += " (рекомендуется)"
        lines.append(line)
        return "\n".join(lines)

    def _keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"Обфускации: {'Строгий' if settings.STRICT_OBFUSCATION else 'Обычный'} ⏼",
                        callback_data="feat:strict:toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Сноски: радиус 0", callback_data="feat:radius:0"
                    ),
                    InlineKeyboardButton("1", callback_data="feat:radius:1"),
                    InlineKeyboardButton("2", callback_data="feat:radius:2"),
                ],
                [
                    InlineKeyboardButton(
                        f"PDF-layout {'on' if settings.PDF_LAYOUT_AWARE else 'off'} ⏼",
                        callback_data="feat:layout:toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"OCR {'on' if settings.ENABLE_OCR else 'off'} ⏼",
                        callback_data="feat:ocr:toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Сбросить к рекомендованным",
                        callback_data="feat:reset:defaults",
                    )
                ],
            ]
        )

    def _doc() -> str:
        return (
            "ℹ️ Рекомендуемые настройки: строгие обфускации — ON, радиус сносок — 1, "
            "PDF-layout — OFF, OCR — OFF."
        )

    await query.answer()
    await query.edit_message_text(
        f"{_status()}\n\n{hint}\n\n{_doc()}", reply_markup=_keyboard()
    )


async def diag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin diagnostic command with runtime information."""

    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        return

    try:
        text = build_diag_text()
    except Exception as exc:  # pragma: no cover - defensive fallback
        text = f"Диагностика: ошибка {type(exc).__name__}: {exc}"
    await notify(update.message, text, event="analysis", force=True)


async def dedupe_log_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Admin command to deduplicate sent log."""

    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        return
    if context.args and context.args[0].lower() in {"yes", "y"}:
        result = mu.dedupe_sent_log_inplace(messaging.LOG_FILE)
        await _safe_reply_text(update.message, str(result))
    else:
        await _safe_reply_text(update.message, 
            "⚠️ Это действие перезапишет sent_log.csv. Запустите /dedupe_log yes для подтверждения."
        )


async def prompt_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt the user to upload files or URLs with e-mail addresses."""

    await _safe_reply_text(update.message, 
        (
            "📥 Загрузите данные с e-mail-адресами для рассылки.\n\n"
            "Поддерживаемые форматы: PDF, Excel (.xlsx), Word (.docx), CSV, "
            "ZIP (с этими файлами внутри), а также ссылки на сайты."
        )
    )


async def about_bot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a short description of the bot."""

    await _safe_reply_text(update.message, 
        (
            "Бот делает рассылку HTML-писем с учётом истории отправки "
            "(IMAP 180 дней) и блок-листа. Один адрес — не чаще 1 раза в 6 "
            "месяцев. Домены: только .ru и .com."
        )
    )


async def stop_process(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the stop button by signalling cancellation."""
    event = context.chat_data.get("cancel_event")
    if event:
        event.set()
    await _safe_reply_text(update.message, "Остановлено…")
    context.chat_data["cancel_event"] = asyncio.Event()


async def add_block_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ask the user to provide e-mails to add to the block list."""

    clear_all_awaiting(context)
    await _safe_reply_text(update.message, 
        (
            "Введите email или список email-адресов "
            "(через запятую/пробел/с новой строки), "
            "которые нужно добавить в исключения:"
        )
    )
    context.user_data["awaiting_block_email"] = True


async def show_blocked_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display the current list of blocked e-mail addresses."""

    dedupe_blocked_file()
    blocked = get_blocked_emails()
    if not blocked:
        await _safe_reply_text(update.message, "📄 Список исключений пуст.")
    else:
        await _safe_reply_text(update.message, 
            "📄 В исключениях:\n" + "\n".join(sorted(blocked))
        )


async def prompt_change_group(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Prompt the user to choose a mailing group."""

    await _safe_reply_text(update.message, 
        "Выберите направление:",
        reply_markup=build_templates_kb(
            context,
            current_code=context.chat_data.get("current_template_code"),
        ),
    )


async def imap_folders_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """List available IMAP folders and allow user to choose."""

    IMAP_HOST = os.getenv("IMAP_HOST", "imap.mail.ru")
    IMAP_TIMEOUT = float(os.getenv("IMAP_TIMEOUT", "15"))
    try:
        imap = imaplib.IMAP4_SSL(IMAP_HOST, timeout=IMAP_TIMEOUT)
        imap.login(messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD)
        status, data = imap.list()
        imap.logout()
        if status != "OK" or not data:
            await _safe_reply_text(update.message, "❌ Не удалось получить список папок.")
            return
        folders = [
            line.decode(errors="ignore").split(' "', 2)[-1].strip('"') for line in data
        ]
        context.user_data["imap_folders"] = folders
        await _show_imap_page(update, context, 0)
    except Exception as e:
        log_error(f"imap_folders_command: {e}")
        await _safe_reply_text(update.message, f"❌ Ошибка IMAP: {e}")


async def _show_imap_page(update_or_query, context, page: int) -> None:
    folders = context.user_data.get("imap_folders", [])
    per_page = 6
    start = page * per_page
    sub = folders[start : start + per_page]
    keyboard = [
        [
            InlineKeyboardButton(
                f,
                callback_data="imap_choose:" + urllib.parse.quote(f, safe=""),
            )
        ]
        for f in sub
    ]
    if len(folders) > per_page:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"imap_page:{page - 1}"))
        if start + per_page < len(folders):
            nav.append(InlineKeyboardButton("➡️", callback_data=f"imap_page:{page + 1}"))
        keyboard.append(nav)
    markup = InlineKeyboardMarkup(keyboard)
    text = "Выберите папку для сохранения отправленных писем:"
    if isinstance(update_or_query, Update):
        await _safe_reply_text(update_or_query.message, text, reply_markup=markup)
    else:
        await update_or_query.message.edit_text(text, reply_markup=markup)


async def imap_page_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    await query.answer()
    page = int(query.data.split(":")[1])
    await _show_imap_page(query, context, page)


async def choose_imap_folder(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    await query.answer()
    encoded = query.data.split(":", 1)[1]
    folder = urllib.parse.unquote(encoded)
    with open(messaging.IMAP_FOLDER_FILE, "w", encoding="utf-8") as f:
        f.write(folder)
    await _safe_reply_text(query.message, f"📁 Папка сохранена: {folder}")


async def force_send_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Enable ignoring of the daily sending limit for this chat."""

    chat_id = update.effective_chat.id
    enable_force_send(chat_id)
    await _safe_reply_text(update.message, 
        "Режим игнорирования дневного лимита включён для этого чата.\n"
        "Запустите рассылку ещё раз — ограничение на сегодня будет проигнорировано."
    )


async def handle_reports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отчёт об отправках за сегодня и неделю."""

    today = summarize_today()
    week = summarize_week()
    tz = current_tz_label()
    lines = [
        f"📝 Отчёт ({tz}):",
        f"Сегодня — ок: {today.get('ok',0)}, ошибок: {today.get('err',0)}",
        f"Неделя — ок: {week.get('ok',0)}, ошибок: {week.get('err',0)}",
    ]
    await _safe_reply_text(update.message, "\n".join(lines))


async def handle_reports_debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Диагностика отчётов: путь, наличие, хвост и текущее время."""

    try:
        p = Path(STATS_PATH)
        exists = p.exists()
        tail: list[str] = []
        if exists:
            with p.open("r", encoding="utf-8") as f:
                lines = f.readlines()[-5:]
                tail = [l.strip() for l in lines]
        now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        msg = [
            "🛠 Диагностика отчётов:",
            f"Файл: {p}",
            f"Существует: {exists}",
            f"Последние записи ({len(tail)}):",
            *tail,
            "",
            f"Время сейчас (UTC): {now_utc}",
            f"TZ отчёта: {current_tz_label()}",
        ]
        try:
            snap = env_snapshot()
            host, port, mode, use_ssl, use_starttls = smtp_settings(snap)
            smtp_result = smtp_ping(host, port, mode, use_ssl=use_ssl, use_starttls=use_starttls)
            imap_host = snap.get("IMAP_HOST") or "imap.mail.ru"
            try:
                imap_port = int(snap.get("IMAP_PORT") or "993")
            except Exception:
                imap_port = 993
            imap_result = imap_ping(imap_host, imap_port)
            msg.extend(
                [
                    "",
                    (
                        "SMTP ping: "
                        f"{'OK' if smtp_result.ok else 'FAIL'} ({smtp_result.latency_ms} ms)"
                        + (f" – {smtp_result.detail}" if not smtp_result.ok else "")
                    ),
                    (
                        "IMAP ping: "
                        f"{'OK' if imap_result.ok else 'FAIL'} ({imap_result.latency_ms} ms)"
                        + (f" – {imap_result.detail}" if not imap_result.ok else "")
                    ),
                ]
            )
        except Exception:
            pass
        await _safe_reply_text(update.message, "\n".join(msg))
    except Exception as e:  # pragma: no cover - best effort
        await _safe_reply_text(update.message, f"Diag error: {e!r}")


# === КНОПКИ ДЛЯ ПОДОЗРИТЕЛЬНЫХ ===
async def on_accept_suspects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    suspects = context.user_data.get("emails_suspects") or []
    if not suspects:
        return await q.edit_message_text("Подозрительных адресов нет.")
    state = get_state(context)
    try:
        text_blob = "\n".join(str(e) for e in suspects if e)
        fixed = parse_emails_unified(text_blob)
    except Exception:
        fixed = [str(e).strip().lower() for e in suspects if e]
    fixed = dedupe_keep_original(fixed)
    fixed = drop_leading_char_twins(fixed)
    fixed = _drop_truncated_twins(fixed, state=state, update_counter=False)
    cleaned = [e for e in fixed if e]
    sendable = set(context.user_data.get("emails_for_sending") or [])
    sendable.update(cleaned)
    context.user_data["emails_for_sending"] = sorted(sendable)
    context.user_data["emails_suspects"] = []
    suspects_lower = {str(e).strip().lower() for e in suspects if e}
    preview = context.chat_data.get("send_preview", {}) or {}
    dropped_list = [
        (addr, reason)
        for addr, reason in preview.get("dropped", [])
        if not (reason == "suspect" and addr.lower() in suspects_lower)
    ]
    preview["dropped"] = dropped_list
    final_list = list(preview.get("final", []))
    final_seen = {addr.lower() for addr in final_list}
    for addr in cleaned:
        if addr.lower() not in final_seen:
            final_list.append(addr)
            final_seen.add(addr.lower())
    preview["final"] = final_list
    context.chat_data["send_preview"] = preview
    current = set(state.to_send)
    current.update(cleaned)
    state.to_send = _drop_truncated_twins(sorted(current), state=state)
    preview_allowed = list(state.preview_allowed_all or [])
    preview_allowed.extend(cleaned)
    state.preview_allowed_all = _drop_truncated_twins(
        sorted(set(preview_allowed)), update_counter=False
    )
    await q.edit_message_text(
        "✅ Подозрительные адреса приняты и добавлены к отправке.\n"
        f"Итого к отправке: {len(sendable)}"
    )


async def on_edit_suspects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    suspects = context.user_data.get("emails_suspects") or []
    preview = "\n".join(suspects[:10]) if suspects else "—"
    await q.edit_message_text(
        "✍️ Введите исправленные e-mail одним блоком (через пробел/запятую/с новой строки).\n"
        "Текущие «подозрительные» (первые 10):\n" + preview
    )
    context.user_data["await_edit_suspects"] = True
    return EDIT_SUSPECTS_INPUT


async def on_edit_suspects_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("await_edit_suspects"):
        return
    text = update.message.text or ""
    # MANUAL FLOW: без автоправок — только базовая нормализация и дедуп по оригиналам
    fixed = parse_emails_unified(text)
    fixed = dedupe_keep_original(fixed)
    sendable = set(context.user_data.get("emails_for_sending") or [])
    for e in fixed:
        sendable.add(e)
    context.user_data["emails_for_sending"] = sorted(sendable)
    context.user_data["emails_suspects"] = []
    context.user_data["await_edit_suspects"] = False
    await _safe_reply_text(update.message, 
        "✅ Исправленные адреса приняты.\n"
        f"Итого к отправке: {len(sendable)}"
    )


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt the user to select a reporting period."""

    keyboard = [
        [InlineKeyboardButton("📆 День", callback_data="report_day")],
        [InlineKeyboardButton("🗓 Неделя", callback_data="report_week")],
        [InlineKeyboardButton("🗓 Месяц", callback_data="report_month")],
        [InlineKeyboardButton("📅 Год", callback_data="report_year")],
    ]
    await _safe_reply_text(update.message, 
        "Выберите период отчёта:", reply_markup=InlineKeyboardMarkup(keyboard)
    )


def get_report(period: str = "day") -> str:
    """Return statistics of sent e-mails for the given period."""
    if period == "day":
        s = summarize_today()
        return f"Успешных: {s.get('ok',0)}\nОшибок: {s.get('err',0)}"
    if period == "week":
        s = summarize_week()
        return f"Успешных: {s.get('ok',0)}\nОшибок: {s.get('err',0)}"

    if not os.path.exists(LOG_FILE):
        return "Нет данных о рассылках."
    now = datetime.now()
    if period == "month":
        start_at = now - timedelta(days=30)
    elif period == "year":
        start_at = now - timedelta(days=365)
    else:
        start_at = now - timedelta(days=1)

    cnt_ok = 0
    cnt_err = 0
    with open(LOG_FILE, encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 4:
                continue
            try:
                dt = datetime.fromisoformat(row[0])
                if dt.tzinfo is not None:
                    dt = dt.replace(tzinfo=None)
            except Exception:
                continue
            if dt >= start_at:
                st = (row[3] or "").strip().lower()
                if st == "ok":
                    cnt_ok += 1
                else:
                    cnt_err += 1
    return f"Успешных: {cnt_ok}\nОшибок: {cnt_err}"


async def report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send the selected report to the user."""

    query = update.callback_query
    await query.answer()
    period = query.data.replace("report_", "")
    mapping = {
        "day": "Отчёт за день",
        "week": "Отчёт за неделю",
        "month": "Отчёт за месяц",
        "year": "Отчёт за год",
    }
    text = get_report(period)
    header = mapping.get(period, period)
    if period in ("day", "week"):
        header = f"{header} ({current_tz_label()})"
    await query.edit_message_text(f"📊 {header}:\n{text}")


async def sync_imap_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Synchronize local log with the IMAP "Sent" folder."""

    await _safe_reply_text(update.message, 
        "⏳ Сканируем папку «Отправленные» (последние 180 дней)..."
    )
    try:
        stats = sync_log_with_imap()
        clear_recent_sent_cache()
        await _safe_reply_text(update.message, 
            "🔄 "
            f"новых: {stats['new_contacts']}, обновлено: {stats['updated_contacts']}, "
            f"пропущено: {stats['skipped_events']}, всего: {stats['total_rows_after']}"
        )
    except Exception as e:
        await _safe_reply_text(update.message, f"❌ Ошибка синхронизации: {e}")


async def sync_bounces_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check INBOX for bounce messages and log them."""
    await _safe_reply_text(update.message, "⏳ Проверяю INBOX на бонсы...")
    try:
        n = sync_bounces()
        await _safe_reply_text(update.message, 
            f"✅ Найдено и добавлено в отчёты: {n} bounce-сообщений."
        )
    except Exception as e:  # pragma: no cover - best effort
        await _safe_reply_text(update.message, f"❌ Ошибка при синхронизации бонсов: {e}")


async def retry_last_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Retry sending e-mails that previously soft-bounced."""

    rows: list[dict] = []
    if BOUNCE_LOG_PATH.exists():
        with BOUNCE_LOG_PATH.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    if not rows:
        await _safe_reply_text(update.message, "Нет писем для ретрая")
        return
    last_ts = rows[-1]["ts"]
    addrs: list[str] = []
    for r in reversed(rows):
        if r["ts"] != last_ts:
            break
        code = r.get("code") or None
        try:
            icode = int(code) if code else None
        except Exception:
            icode = None
        if is_soft_bounce(icode, r.get("msg")):
            email = (r.get("email") or "").lower().strip()
            if email:
                addrs.append(email)
    unique = list(dict.fromkeys(addrs))
    if not unique:
        await _safe_reply_text(update.message, "Нет писем для ретрая")
        return
    sent = 0
    for addr in unique:
        if is_suppressed(addr):
            continue
        try:
            messaging.send_raw_smtp_with_retry("retry", addr)
            log_sent_email(addr, "retry")
            sent += 1
        except Exception as e:
            logger.warning("retry_last send failed for %s: %s", addr, e)
    await _safe_reply_text(update.message, f"Повторно отправлено: {sent}")


async def reset_email_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clear stored e-mails and reset the session state."""

    chat_id = update.effective_chat.id
    init_state(context)
    context.chat_data.pop("manual_all_emails", None)
    context.chat_data.pop("send_preview", None)
    context.chat_data.pop("fix_pending", None)
    context.chat_data["batch_id"] = None
    mass_state.clear_batch(chat_id)
    context.chat_data["extract_lock"] = asyncio.Lock()
    await _safe_reply_text(update.message, 
        "Список email-адресов и файлов очищен. Можно загружать новые файлы!"
    )


async def _compose_report_and_save(
    context: ContextTypes.DEFAULT_TYPE,
    allowed_all: Set[str],
    filtered: List[str],
    dropped: List[Tuple[str, str]],
    foreign: List[str],
    footnote_dupes: int = 0,
    cooldown_blocked: Sequence[str] | None = None,
) -> str:
    """Compose a summary report and store samples in session state."""

    state = get_state(context)

    combined_emails: list[str] = []
    for addr in allowed_all:
        if isinstance(addr, str):
            combined_emails.append(addr)
    for addr in foreign:
        if isinstance(addr, str):
            combined_emails.append(addr)

    classes = _classify_emails(
        combined_emails,
        dropped=dropped,
        cooldown_candidates=cooldown_blocked,
    )

    S_all: Set[str] = classes["all"]
    S_sus: Set[str] = classes["sus"]
    S_foreign: Set[str] = classes["foreign"]
    S_cool: Set[str] = classes["cool"]
    S_send: Set[str] = classes["send"]
    reason_map: Dict[str, str] = classes["reasons"]
    originals: Dict[str, str] = classes["original"]
    dropped_order: List[str] = classes["dropped_order"]

    state_map = context.chat_data.setdefault("history_snapshot", {})

    def _history_key(norm: str) -> str:
        original = originals.get(norm, norm)
        return messaging._normalize_key(original)

    frozen_to_send = []
    frozen_all = []
    frozen_reason_map: Dict[str, str] = {}
    frozen_original_map: Dict[str, str] = {}

    for norm in sorted(S_all):
        key = _history_key(norm)
        if not key:
            continue
        frozen_all.append(key)
        if norm in S_cool:
            category = "cooldown"
        elif norm in S_sus:
            category = "suspect"
        elif norm in S_foreign:
            category = "foreign"
        else:
            category = "clean"
        frozen_reason_map[key] = category
        frozen_original_map.setdefault(key, originals.get(norm, norm))

    for norm in sorted(S_send):
        key = _history_key(norm)
        if not key:
            continue
        frozen_to_send.append(key)
        frozen_original_map.setdefault(key, originals.get(norm, norm))

    state_map["frozen_to_send"] = frozen_to_send
    state_map["frozen_all"] = frozen_all
    state_map["frozen_reason_map"] = frozen_reason_map
    state_map["frozen_original_map"] = frozen_original_map

    for addr in foreign:
        norm = _normalize_email_lower(addr)
        if not norm:
            continue
        S_foreign.add(norm)
        S_all.add(norm)
        originals.setdefault(norm, str(addr).strip())

    def _restore(addresses: Iterable[str]) -> List[str]:
        restored: List[str] = []
        for addr in addresses:
            restored.append(originals.get(addr, addr))
        return restored

    final_send = _restore(sorted(S_send))
    final_send = _drop_truncated_twins(final_send, state=state, update_counter=False)
    foreign_list = _restore(sorted(S_foreign))
    cooldown_list = _restore(sorted(S_cool))

    unique_dropped: List[Tuple[str, str]] = []
    for norm in dropped_order:
        addr = originals.get(norm, norm)
        reason = reason_map.get(norm, "")
        unique_dropped.append((addr, reason))

    state.preview_allowed_all = final_send
    state.to_send = final_send
    state.dropped = unique_dropped
    state.foreign = foreign_list
    state.cooldown_blocked = cooldown_list
    state.footnote_dupes = footnote_dupes

    context.user_data["emails_for_sending"] = list(final_send)

    context.chat_data["send_preview"] = {
        "final": final_send,
        "dropped": unique_dropped,
        "fixed": [],
        "cooldown_blocked": cooldown_list,
    }
    context.chat_data.pop("fix_pending", None)

    sample_allowed = sample_preview(final_send, PREVIEW_ALLOWED)
    sample_foreign = sample_preview(foreign_list, PREVIEW_FOREIGN)

    total_allowed_count = len(S_all - S_foreign)

    report_lines = [
        "✅ Анализ завершён.",
        f"Найдено адресов: {total_allowed_count}",
        f"📧 К отправке: {len(S_send)} адресов",
        f"⚠️ Подозрительные: {len(S_sus)} адресов",
        f"🕒 Под кулдауном (180 дней): {len(S_cool)} адресов",
        f"🌍 Иностранные домены: {len(S_foreign)}",
        f"🧭 Просмотрено страниц: {int(context.chat_data.get('crawl_pages', 0))}",
        f"Возможные сносочные дубликаты удалены: {footnote_dupes}",
    ]
    if len(S_send) == 0 and (
        len(S_cool) > 0
        or any(messaging._should_skip_by_history(addr)[0] for addr in S_all)
    ):
        report_lines.append(
            "ℹ️ Почти все адреса исключены историей/блок-листами. Проверьте период 180 дней и suppress."
        )
    report = "\n".join(report_lines)

    if sample_allowed:
        report += "\n\n🧪 Примеры:\n" + "\n".join(sample_allowed)

    sus_preview: List[Tuple[str, str]] = []
    seen_sus: Set[str] = set()
    for addr, reason in unique_dropped:
        norm = _normalize_email_lower(addr)
        if norm not in S_sus or norm in seen_sus:
            continue
        sus_preview.append((addr, reason))
        seen_sus.add(norm)
        if len(sus_preview) >= 10:
            break
    if sus_preview:
        preview_lines = ["\n⚠️ Подозрительные адреса:"]
        for idx, (addr, reason) in enumerate(sus_preview, 1):
            suffix = f" — {reason}" if reason else ""
            preview_lines.append(f"{idx}) {addr}{suffix}")
        report += "\n" + "\n".join(preview_lines)
        if C.ALLOW_EDIT_AT_PREVIEW:
            report += "\nНажмите «✏️ Исправить №…» чтобы отредактировать."

    if cooldown_list:
        sample_cooldown = cooldown_list[: min(10, len(cooldown_list))]
        report += "\n\n🕒 Примеры адресов под кулдауном:\n" + "\n".join(sample_cooldown)

    if sample_foreign:
        report += "\n\n🌍 Примеры иностранных:\n" + "\n".join(sample_foreign)

    return report


async def request_fix(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt the user to provide a fixed e-mail address."""

    query = update.callback_query
    await query.answer()
    preview = context.chat_data.get("send_preview", {})
    dropped = preview.get("dropped", [])
    data = query.data or ""
    try:
        _, idx_s = data.split(":", 1)
        idx = int(idx_s)
    except Exception:
        await _safe_reply_text(query.message, "⚠️ Некорректный индекс.")
        return
    if idx < 0 or idx >= len(dropped):
        await _safe_reply_text(query.message, "⚠️ Индекс вне диапазона.")
        return
    original, reason = dropped[idx]
    context.chat_data["fix_pending"] = {"index": idx, "original": original}
    await _safe_reply_text(query.message, 
        (
            "Введите исправленный адрес для:\n"
            f"`{original}`\n(прежняя причина: {reason})"
        ),
        parse_mode="Markdown",
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle an uploaded document with potential e-mail addresses."""

    doc = update.message.document
    if not doc:
        return
    chat_id = update.effective_chat.id
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(
        DOWNLOAD_DIR, f"{chat_id}_{int(time.time())}_{doc.file_name}"
    )
    f = await doc.get_file()
    await f.download_to_drive(file_path)

    await notify(update.message, "Файл загружен. Идёт анализ...", event="analysis")
    await notify(update.message, "🔎 Анализируем...", event="analysis")

    allowed_all, loose_all = set(), set()
    extracted_files: List[str] = []
    repairs: List[tuple[str, str]] = []
    footnote_dupes = 0
    suspicious_map: Dict[str, str] = {}
    role_rejected_total = 0

    try:
        if file_path.lower().endswith(".zip"):
            allowed, extracted_files, loose, stats = await extract_emails_from_zip(
                file_path
            )
            allowed_all.update(allowed)
            loose_all.update(loose)
            repairs = collect_repairs_from_files(extracted_files)
            footnote_dupes += stats.get("footnote_pairs_merged", 0)
            _ingest_meta_to(loose_all, suspicious_map, stats)
            if isinstance(stats, dict):
                try:
                    role_rejected_total += int(stats.get("role_rejected", 0) or 0)
                except Exception:
                    pass
        else:
            allowed, loose, stats = extract_from_uploaded_file(file_path)
            allowed_all.update(allowed)
            loose_all.update(loose)
            extracted_files.append(file_path)
            repairs = collect_repairs_from_files([file_path])
            footnote_dupes += stats.get("footnote_pairs_merged", 0)
            _ingest_meta_to(loose_all, suspicious_map, stats)
            if isinstance(stats, dict):
                try:
                    role_rejected_total += int(stats.get("role_rejected", 0) or 0)
                except Exception:
                    pass
    except Exception as e:
        log_error(f"handle_document: {file_path}: {e}")

    allowed_all, trunc_pairs = apply_numeric_truncation_removal(allowed_all)
    repairs = list(dict.fromkeys(repairs + trunc_pairs))

    technical_emails = [e for e in allowed_all if any(tp in e for tp in TECH_PATTERNS)]
    filtered = [
        e for e in allowed_all if e not in technical_emails and is_allowed_tld(e)
    ]
    meta_suspects = {addr for addr, reason in suspicious_map.items() if reason == "suspect"}
    heuristics_suspects = {addr for addr in filtered if _is_suspect_email(addr)}
    suspects_set: Set[str] = set(meta_suspects) | heuristics_suspects
    filtered_set = set(filtered)
    suspects_removed = suspects_set & filtered_set
    if SUSPECTS_REQUIRE_CONFIRM and suspects_removed:
        filtered = [addr for addr in filtered if addr not in suspects_removed]

    dropped_current: List[Tuple[str, str]] = []
    for email in sorted(allowed_all):
        if email in filtered:
            continue
        if email in technical_emails:
            dropped_current.append((email, "technical-address"))
        elif not is_allowed_tld(email):
            dropped_current.append((email, "foreign-domain"))
        else:
            dropped_current.append((email, "filtered"))

    suspicious_items = sorted(suspicious_map.items())
    for candidate, reason in suspicious_items:
        if reason == "suspect" and not SUSPECTS_REQUIRE_CONFIRM:
            continue
        dropped_current.append((candidate, reason))
    if SUSPECTS_REQUIRE_CONFIRM and suspects_removed:
        existing_suspects = {addr for addr, reason in suspicious_items if reason == "suspect"}
        heuristics_only = sorted(suspects_removed - existing_suspects)
        for addr in heuristics_only:
            dropped_current.append((addr, "suspect"))

    all_found = {addr for addr in allowed_all | loose_all if addr}
    foreign_raw = {
        addr
        for addr in all_found
        if "@" in addr and not _is_allowed_domain(addr.rsplit("@", 1)[-1])
    }
    foreign = sorted(collapse_footnote_variants(foreign_raw))

    preview_stats = {
        "suspicious_count": len(suspicious_map),
        "foreign_domains": len(foreign_raw),
        "role_rejected": role_rejected_total,
    }
    preview_message: str | None = None
    if all_found or filtered or preview_stats["suspicious_count"] or preview_stats["role_rejected"]:
        preview_message = _format_preview_text(
            all_found,
            filtered,
            dropped_current,
            preview_stats,
        )

    state = get_state(context)
    state.all_emails.update(allowed_all)
    state.all_files.extend(extracted_files)
    current = set(state.to_send)
    current.update(filtered)
    state.to_send = _drop_truncated_twins(sorted(current), state=state)
    context.user_data["emails_for_sending"] = list(state.to_send)
    state.repairs = list(dict.fromkeys((state.repairs or []) + repairs))
    state.repairs_sample = sample_preview([f"{b} → {g}" for (b, g) in state.repairs], 6)
    all_allowed = state.all_emails
    foreign_total = set(state.foreign) | set(foreign)
    total_footnote = state.footnote_dupes + footnote_dupes

    existing = list(state.dropped or [])
    combined_map: dict[str, str] = {}
    for addr, reason in existing + dropped_current:
        if addr not in combined_map:
            combined_map[addr] = reason
    dropped_total = [(addr, combined_map[addr]) for addr in combined_map]

    try:
        cooldown_blocked = [
            addr
            for addr in sorted(
                {
                    str(candidate).strip().lower()
                    for candidate in all_allowed
                    if isinstance(candidate, str) and "@" in candidate
                }
            )
            if messaging._should_skip_by_history(addr)[0]
        ]
    except Exception:
        cooldown_blocked = []

    report = await _compose_report_and_save(
        context,
        all_allowed,
        state.to_send,
        dropped_total,
        sorted(foreign_total),
        total_footnote,
        cooldown_blocked,
    )
    if SUSPECTS_REQUIRE_CONFIRM and suspects_removed:
        context.user_data["emails_suspects"] = sorted(suspects_removed)
    else:
        context.user_data["emails_suspects"] = []
    context.user_data["await_edit_suspects"] = False
    if state.repairs_sample:
        report += "\n\n🧩 Возможные исправления (проверьте вручную):"
        for s in state.repairs_sample:
            report += f"\n{s}"
    preview = context.chat_data.get("send_preview", {})
    dropped_preview = preview.get("dropped", [])
    extra_buttons: List[List[InlineKeyboardButton]] = []
    if C.ALLOW_EDIT_AT_PREVIEW:
        fix_buttons: List[InlineKeyboardButton] = []
        for idx in range(min(len(dropped_preview), 5)):
            fix_buttons.append(
                InlineKeyboardButton(
                    f"✏️ Исправить №{idx + 1}", callback_data=f"fix:{idx}"
                )
            )
        if fix_buttons:
            extra_buttons.append(fix_buttons)
    extra_buttons.append(
        [
            InlineKeyboardButton(
                "🔁 Показать ещё примеры", callback_data="refresh_preview"
            )
        ]
    )
    suspects_preview = [addr for addr, reason in dropped_preview if reason == "suspect"]
    if SUSPECTS_REQUIRE_CONFIRM and suspects_preview:
        extra_buttons.append(
            [
                InlineKeyboardButton(
                    "✅ Принять подозрительные", callback_data="accept_suspects"
                ),
                InlineKeyboardButton(
                    "✍️ Исправить адреса", callback_data="edit_suspects"
                ),
            ]
        )
    if state.repairs:
        extra_buttons.append(
            [
                InlineKeyboardButton(
                    f"🧩 Применить исправления ({len(state.repairs)})",
                    callback_data="apply_repairs",
                )
            ]
        )
        extra_buttons.append(
            [
                InlineKeyboardButton(
                    "🧩 Показать все исправления", callback_data="show_repairs"
                )
            ]
        )
    extra_buttons.append(
        [
            InlineKeyboardButton(
                "▶️ Перейти к выбору направления", callback_data="proceed_group"
            )
        ]
    )
    report += "\n\nДополнительные действия:"
    await notify(
        update.message,
        report,
        reply_markup=InlineKeyboardMarkup(extra_buttons),
        event="analysis",
        force=True,
    )
    if preview_message:
        await _safe_reply_text(update.message, preview_message)


async def refresh_preview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a fresh sample of extracted e-mail addresses."""

    query = update.callback_query
    state = context.chat_data.get(SESSION_KEY)
    allowed_all = state.preview_allowed_all if state else []
    dropped = state.dropped if state else []
    foreign = state.foreign if state else []
    if not (allowed_all or dropped or foreign):
        await query.answer(
            "Нет данных для примеров. Загрузите файл/ссылки.", show_alert=True
        )
        return
    await query.answer()
    sample_allowed = sample_preview(allowed_all, PREVIEW_ALLOWED)
    sample_foreign = sample_preview(foreign, PREVIEW_FOREIGN)
    report = []
    if sample_allowed:
        report.append("🧪 Примеры:\n" + "\n".join(sample_allowed))
    if dropped:
        preview_lines = [
            f"{i + 1}) {addr} — {reason}"
            for i, (addr, reason) in enumerate(dropped[:5])
        ]
        if preview_lines:
            report.append("⚠️ Подозрительные:\n" + "\n".join(preview_lines))
    if sample_foreign:
        report.append("🌍 Примеры иностранных:\n" + "\n".join(sample_foreign))
    await _safe_reply_text(query.message, 
        "\n\n".join(report) if report else "Показать нечего."
    )


async def prompt_manual_email(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Ask the user to enter e-mail addresses manually."""

    clear_all_awaiting(context)
    context.chat_data.pop("manual_all_emails", None)
    context.chat_data.pop("manual_send_mode", None)
    context.chat_data.pop("manual_allowed_preview", None)
    context.chat_data.pop("manual_rejected_preview", None)
    context.chat_data.pop("manual_selected_template_code", None)
    context.chat_data.pop("manual_selected_template_label", None)
    context.chat_data.pop("manual_selected_emails", None)
    context.chat_data.pop("manual_drop_reasons", None)
    _manual_override_clear(context)
    await _safe_reply_text(update.message,
        (
            "Введите email или список email-адресов "
            "(через запятую/пробел/с новой строки):"
        )
    )
    context.user_data["awaiting_manual_email"] = True


def _norm_prefix(value: str) -> str:
    """Normalize section prefix ensuring it starts with ``/``."""

    raw = (value or "").strip()
    if not raw:
        return ""
    return raw if raw.startswith("/") else "/" + raw


def _first_url(text: str | None) -> str | None:
    """Return the first URL found in ``text`` or ``None`` if absent."""

    match = URL_RX.search(text or "")
    return match.group(0) if match else None


async def handle_text_with_url(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    """Show mode selection keyboard when a text message contains a URL."""

    message = update.message
    if not message or not message.text:
        return False
    url = _first_url(message.text)
    if not url:
        return False

    token = secrets.token_hex(6)
    mapping = context.user_data.setdefault("parse_mode_urls", {})
    mapping[token] = url
    tokens_by_message = context.user_data.setdefault(
        "parse_mode_tokens_by_message", {}
    )
    try:
        prompt = await message.reply_text(
            f"Нашла ссылку:\n{url}\nКак парсить?",
            reply_markup=build_parse_mode_kb(token),
        )
    except Exception:
        mapping.pop(token, None)
        return False
    if hasattr(prompt, "message_id"):
        tokens_by_message[prompt.message_id] = token
    # keep mapping reasonably small
    if len(mapping) > 24:
        for stale in list(mapping)[:-24]:
            mapping.pop(stale, None)
    if len(tokens_by_message) > 48:
        for stale_id in list(tokens_by_message)[:-48]:
            token_id = tokens_by_message.pop(stale_id, None)
            if token_id and token_id not in tokens_by_message.values():
                mapping.pop(token_id, None)
    return True


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process text messages for uploads, blocking or manual lists."""

    chat_id = update.effective_chat.id
    text = update.message.text or ""
    if context.chat_data.get("preview_edit_pending"):
        from emailbot.handlers import preview as preview_handlers

        await preview_handlers.handle_edit_input(update, context)
        return
    fix_state = context.chat_data.get("fix_pending")
    if fix_state:
        new_text = text.strip()
        if not new_text:
            await _safe_reply_text(update.message, "❌ Введите корректный адрес.")
            return
        from pipelines.extract_emails import run_pipeline_on_text

        final_new, dropped_new = run_pipeline_on_text(new_text)
        if final_new and not dropped_new:
            new_email = final_new[0]
            preview = context.chat_data.get("send_preview", {}) or {}
            dropped_list = list(preview.get("dropped", []))
            idx = fix_state.get("index", -1)
            original = fix_state.get("original")
            if 0 <= idx < len(dropped_list) and dropped_list[idx][0] == original:
                dropped_list.pop(idx)
            else:
                dropped_list = [pair for pair in dropped_list if pair[0] != original]
            preview["dropped"] = dropped_list
            final_list = [
                item for item in list(preview.get("final", [])) if item != original
            ]
            final_list.append(new_email)
            preview["final"] = _drop_truncated_twins(
                list(dict.fromkeys(final_list)), update_counter=False
            )
            fixed_list = list(preview.get("fixed", []))
            fixed_list.append({"from": original, "to": new_email})
            preview["fixed"] = fixed_list
            context.chat_data["send_preview"] = preview
            context.chat_data.pop("fix_pending", None)

            state = get_state(context)
            state.dropped = [pair for pair in state.dropped if pair[0] != original]
            state.foreign = sorted(addr for addr in state.foreign if addr != original)
            to_send_set = set(state.to_send)
            to_send_set.discard(original)
            to_send_set.add(new_email)
            state.to_send = _drop_truncated_twins(
                sorted(to_send_set), state=state
            )
            preview_allowed = [
                addr for addr in state.preview_allowed_all if addr != original
            ]
            preview_allowed.append(new_email)
            state.preview_allowed_all = _drop_truncated_twins(
                sorted(set(preview_allowed)), update_counter=False
            )
            await _safe_reply_text(update.message, 
                f"✅ Исправлено: `{original}` → **{new_email}**",
                parse_mode="Markdown",
            )
        else:
            reason = dropped_new[0][1] if dropped_new else "invalid"
            await _safe_reply_text(update.message, 
                f"❌ Всё ещё некорректно ({reason}). Попробуйте ещё раз или отправьте другой адрес."
            )
        return
    if context.user_data.get("awaiting_block_email"):
        clean = _preclean_text_for_emails(text)
        emails = {normalize_email(x) for x in extract_emails_loose(clean) if "@" in x}
        added = [e for e in emails if add_blocked_email(e)]
        await _safe_reply_text(update.message, 
            f"Добавлено в исключения: {len(added)}" if added else "Ничего не добавлено."
        )
        context.user_data["awaiting_block_email"] = False
        return
    if context.user_data.get("awaiting_manual_email"):
        final_emails, stats = extract_emails_pipeline(text)
        pre_trunc = list(final_emails)
        preview_message: str | None = None
        emails_no_trunc = _drop_truncated_twins(pre_trunc, state=get_state(context))
        truncated_removed = [
            addr for addr in pre_trunc if addr not in set(emails_no_trunc)
        ]
        emails = sorted(emails_no_trunc, key=str.lower)
        if not emails:
            fallback_matches = sorted(
                {m.group(0).lower() for m in _FALLBACK_EMAIL_RX.finditer(text)}
            )
            if fallback_matches:
                emails = fallback_matches
                truncated_removed = []
                drop_details = []
                logger.info(
                    "Manual input fallback regex: raw=%r emails=%r", text, emails
                )
            else:
                drop_details = _manual_collect_drop_reasons(
                    stats, emails, truncated_removed
                )
                if drop_details:
                    preview_lines = [
                        f"{addr} — {reason}" for addr, reason in drop_details[:50]
                    ]
                    await _safe_reply_text(update.message,
                        "Исключены адреса и причины:\n" + "\n".join(preview_lines),
                    )
                await _safe_reply_text(update.message, "❌ Не найдено ни одного email.")
                return
        else:
            drop_details = _manual_collect_drop_reasons(
                stats, emails, truncated_removed
            )
            logger.info("Manual input parsing: raw=%r emails=%r", text, emails)

        if emails:
            preview_stats = dict(stats) if isinstance(stats, dict) else {}
            preview_found = _collect_preview_found(preview_stats)
            if preview_found or preview_stats or emails:
                preview_message = _format_preview_text(
                    preview_found,
                    emails,
                    preview_stats.get("items_rejected"),
                    preview_stats,
                )

        # Скрываем список адресов: считаем только количества
        context.user_data["awaiting_manual_email"] = False
        context.chat_data["manual_all_emails"] = emails
        context.chat_data["manual_send_mode"] = "allowed"  # allowed|all
        context.chat_data["manual_drop_reasons"] = drop_details

        template_rows = [
            row[:]
            for row in build_templates_kb(
                context,
                current_code=context.chat_data.get("manual_selected_template_code"),
                prefix="manual_tpl:",
            ).inline_keyboard
        ]

        enforce, days, allow_override = _manual_cfg()
        if enforce:
            allowed, rejected = _filter_by_180(emails, group="", days=days, chat_id=chat_id)
        else:
            allowed, rejected = (emails, [])

        context.chat_data["manual_allowed_preview"] = allowed
        context.chat_data["manual_rejected_preview"] = rejected
        if allow_override and enforce and rejected:
            _manual_override_prepare(context, rejected, days)
        else:
            _manual_override_clear(context)

        lines = ["Адреса получены.", f"К отправке (предварительно): {len(allowed)}"]
        if rejected:
            lines.append(f"Отфильтровано по правилу {days} дней: {len(rejected)}")
        selected_override = _manual_override_selected_set(context)
        if selected_override:
            lines.append(
                f"Для игнорирования выбрано адресов: {len(selected_override)}"
            )

        mode_row: list[InlineKeyboardButton] = []
        if allow_override and rejected:
            mode_row = [
                InlineKeyboardButton(
                    "Отправить только разрешённым", callback_data="manual_mode_allowed"
                ),
                InlineKeyboardButton("Отправить всем", callback_data="manual_mode_all"),
            ]
        ignore_row: list[InlineKeyboardButton] = []
        if allow_override and enforce and rejected:
            ignore_row = [
                InlineKeyboardButton(
                    "Игнорировать (выбранные)",
                    callback_data="manual_ignore_selected:go",
                )
            ]
        keyboard = [*template_rows]
        if mode_row:
            keyboard.append(mode_row)
        if ignore_row:
            keyboard.append(ignore_row)
        keyboard.append([InlineKeyboardButton("♻️ Сброс", callback_data="manual_reset")])

        await _safe_reply_text(update.message,
            "\n".join(lines) + "\n\n⬇️ Выберите направление письма:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        if preview_message:
            await _safe_reply_text(update.message, preview_message)
        if drop_details:
            preview_lines = [
                f"{addr} — {reason}" for addr, reason in drop_details[:50]
            ]
            await _safe_reply_text(update.message,
                "Исключены адреса и причины:\n" + "\n".join(preview_lines),
            )
        return

    if await handle_text_with_url(update, context):
        return


async def page_url_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Однократный парсинг страницы по команде /page <URL>."""

    args = context.args or []
    message = update.effective_message
    text = message.text if message and message.text else ""
    candidate = args[0] if args else _first_url(text)
    url = (candidate or "").strip()
    if not url:
        if message:
            await _safe_reply_text(
                message,
                "Укажи ссылку: /page https://пример.ру/статья",
            )
        return
    await _run_url_extraction(update, context, url, deep=False)


async def sections_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Глубокий парсинг только выбранных разделов: /sections <URL> <paths>."""

    args = context.args or []
    message = update.effective_message
    text = message.text if message and message.text else ""
    candidate = args[0] if args else _first_url(text)
    url = (candidate or "").strip()
    if not url:
        if message:
            await _safe_reply_text(
                message,
                "Укажи адрес сайта и разделы: /sections https://example.com /news,/authors",
            )
        return

    if len(args) > 1:
        raw = " ".join(args[1:])
    else:
        raw = text.replace(url, "", 1)
    parts = re.split(r"[;,\s]+", raw)
    prefixes: list[str] = []
    for part in parts:
        normalized = _norm_prefix(part)
        if normalized:
            prefixes.append(normalized)
    prefixes = list(dict.fromkeys(prefixes))
    if not prefixes:
        if message:
            await _safe_reply_text(
                message,
                "Укажи хотя бы один раздел, например: /sections https://example.com /news,/journals",
            )
        return

    await _run_url_extraction(
        update,
        context,
        url,
        deep=True,
        path_prefixes=prefixes,
    )


async def parse_mode_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик inline-кнопок выбора режима парсинга."""

    query = update.callback_query
    if not query:
        return
    await query.answer()
    data = query.data or ""
    parts = data.split("|", 2)
    if len(parts) < 3:
        try:
            await query.edit_message_text("Не поняла выбор. Повтори, пожалуйста.")
        except Exception:
            pass
        return
    _, mode, token = parts
    mapping = context.user_data.get("parse_mode_urls", {})
    tokens_by_message = context.user_data.get("parse_mode_tokens_by_message", {})
    if query.message and hasattr(query.message, "message_id"):
        tokens_by_message.pop(query.message.message_id, None)
    url = mapping.pop(token, None)
    if not url:
        await query.answer(
            "Ссылка потерялась, отправь снова, пожалуйста.", show_alert=True
        )
        return
    deep = mode == "deep"
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    origin_message_id = (
        query.message.message_id
        if query.message and hasattr(query.message, "message_id")
        else None
    )
    await _run_url_extraction(
        update,
        context,
        url,
        deep=deep,
        origin_message_id=origin_message_id,
    )


async def _run_url_extraction(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    url: str,
    *,
    deep: bool,
    origin_message_id: int | None = None,
    path_prefixes: Sequence[str] | None = None,
) -> None:
    """Общий раннер: запускает парсинг URL и управляет прогрессом/выводом."""

    message = update.effective_message
    chat = update.effective_chat
    chat_id = chat.id if chat else None
    clean_url = (url or "").strip()
    if not clean_url:
        if message:
            await _safe_reply_text(message, "Ссылка не распознана. Пришли ещё раз.")
        return
    if chat_id is None and message:
        chat_id = message.chat_id

    prefixes_list: list[str] = []
    if path_prefixes:
        seen: list[str] = []
        for raw in path_prefixes:
            try:
                normalized = _norm_prefix(str(raw))
            except Exception:
                normalized = ""
            if not normalized:
                continue
            if normalized not in seen:
                seen.append(normalized)
        prefixes_list = seen
    filters_line = (
        f"Фильтры разделов: {', '.join(prefixes_list)}\n" if prefixes_list else ""
    )

    lock = context.chat_data.setdefault("extract_lock", asyncio.Lock())
    if lock.locked():
        if message:
            await _safe_reply_text(message, "⏳ Уже идёт анализ этого URL")
        return

    now = time.monotonic()
    last = context.chat_data.get("last_url")
    if last:
        last_url_value = str(last.get("url") or "")
        try:
            last_ts = float(last.get("ts", 0.0) or 0.0)
        except Exception:
            last_ts = 0.0
        last_deep = bool(last.get("deep", True))
        if (
            last_url_value == clean_url
            and last_deep == bool(deep)
            and now - last_ts < 10.0
        ):
            if message:
                await _safe_reply_text(message, "⏳ Уже идёт анализ этого URL")
            return

    context.chat_data["last_url"] = {"url": clean_url, "deep": deep, "ts": now}
    batch_id = secrets.token_hex(8)
    context.chat_data["batch_id"] = batch_id
    if chat_id is not None:
        mass_state.set_batch(chat_id, batch_id)
    _extraction_url.set_batch(batch_id)
    context.chat_data["entry_url"] = clean_url
    context.chat_data["crawl_pages"] = 0

    if origin_message_id is not None and chat_id is not None:
        try:
            await context.bot.edit_message_reply_markup(
                chat_id=chat_id, message_id=origin_message_id, reply_markup=None
            )
        except Exception:
            pass

    mode_label = "🕸️ Сканируем сайт" if deep else "📄 Парсим страницу"
    status_chat_id = chat_id
    status_message_id: int | None = None
    if message is not None:
        try:
            status_msg = await message.reply_text(
                f"{mode_label}…\n"
                f"Просмотрено страниц: 0\n"
                f"{filters_line}"
                f"Последняя: {clean_url}"
            )
            status_chat_id = status_msg.chat_id
            status_message_id = status_msg.message_id
        except Exception:
            status_msg = None
    elif chat_id is not None:
        try:
            status_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"{mode_label}…\n"
                    f"Просмотрено страниц: 0\n"
                    f"{filters_line}"
                    f"Последняя: {clean_url}"
                ),
            )
            status_chat_id = status_msg.chat_id
            status_message_id = status_msg.message_id
        except Exception:
            status_msg = None
    else:
        status_msg = None

    visited = 0
    last_url_seen = clean_url
    last_edit_ts = 0.0
    progress_cancelled = False
    try:
        max_updates = int(
            os.getenv("PROGRESS_MAX_UPDATES_PER_MINUTE", "100") or "100"
        )
    except Exception:
        max_updates = 100
    max_updates = max(1, min(300, max_updates))
    min_interval = 60.0 / float(max_updates)

    def progress_cb(count: int, current_url: str) -> None:
        nonlocal visited, last_url_seen, last_edit_ts, progress_cancelled
        if progress_cancelled:
            return
        visited = count
        if current_url:
            last_url_seen = current_url
        now_ts = time.time()
        if now_ts - last_edit_ts < min_interval:
            return
        last_edit_ts = now_ts
        if status_message_id is None or status_chat_id is None:
            return

        async def _apply() -> None:
            try:
                await context.bot.edit_message_text(
                    chat_id=status_chat_id,
                    message_id=status_message_id,
                    text=(
                        f"{mode_label}…\n"
                        f"Просмотрено страниц: {visited}\n"
                        f"{filters_line}"
                        f"Последняя: {last_url_seen}"
                    ),
                )
            except Exception:
                pass

        try:
            asyncio.create_task(_apply())
        except Exception:
            pass

    results: list[tuple] = []
    extract_fn = async_extract_emails_from_url
    sig = inspect.signature(extract_fn)
    accepts_deep = "deep" in sig.parameters
    accepts_progress = "progress_cb" in sig.parameters
    accepts_prefixes = "path_prefixes" in sig.parameters
    error: Exception | None = None
    async with lock:
        async with aiohttp.ClientSession() as session:
            kwargs: dict[str, object] = {}
            if accepts_deep:
                kwargs["deep"] = deep
            if accepts_progress:
                kwargs["progress_cb"] = progress_cb
            if accepts_prefixes and prefixes_list:
                kwargs["path_prefixes"] = list(prefixes_list)
            try:
                result = await extract_fn(
                    clean_url, session, chat_id, batch_id, **kwargs
                )
                results = [result]
            except Exception as exc:
                error = exc
    progress_cancelled = True

    if error is not None:
        logger.exception("URL extraction failed: %s", error)
        error_text = str(error) or error.__class__.__name__
        if len(error_text) > 180:
            error_text = error_text[:177] + "…"
        if status_message_id is not None and status_chat_id is not None:
            try:
                await context.bot.edit_message_text(
                    chat_id=status_chat_id,
                    message_id=status_message_id,
                    text=f"❌ Ошибка при разборе: {error_text}",
                )
            except Exception:
                pass
        elif message:
            await _safe_reply_text(message, f"❌ Ошибка при разборе: {error_text}")
        return

    if batch_id != context.chat_data.get("batch_id"):
        return

    total_pages = 0
    stats_sequence: list[dict] = []
    for result in results:
        if not isinstance(result, tuple) or len(result) < 5:
            continue
        stats = result[4]
        if isinstance(stats, dict):
            stats_sequence.append(stats)
            try:
                total_pages += int(stats.get("pages", 0) or 0)
            except Exception:
                pass
    context.chat_data["crawl_pages"] = total_pages

    allowed_all: Set[str] = set()
    foreign_all: Set[str] = set()
    repairs_all: List[tuple[str, str]] = []
    footnote_dupes = 0
    loose_meta: Set[str] = set()
    suspicious_map: Dict[str, str] = {}
    role_rejected_total = 0
    for entry in results:
        if not isinstance(entry, tuple) or len(entry) < 5:
            continue
        _, allowed, foreign, repairs, stats = entry
        allowed_all.update(allowed)
        foreign_all.update(foreign)
        repairs_all.extend(repairs)
        if isinstance(stats, dict):
            footnote_dupes += int(stats.get("footnote_pairs_merged", 0) or 0)
        _ingest_meta_to(loose_meta, suspicious_map, stats)
        if isinstance(stats, dict):
            try:
                role_rejected_total += int(stats.get("role_rejected", 0) or 0)
            except Exception:
                pass

    if loose_meta:
        extra_foreign = {
            addr
            for addr in loose_meta
            if addr and "@" in addr and not _is_allowed_domain(addr.rsplit("@", 1)[-1])
        }
        foreign_all.update(extra_foreign)

    technical_emails = [
        e for e in allowed_all if any(tp in e for tp in TECH_PATTERNS)
    ]
    filtered = [
        e for e in allowed_all if e not in technical_emails and is_allowed_tld(e)
    ]
    meta_suspects = {
        addr for addr, reason in suspicious_map.items() if reason == "suspect"
    }
    heuristics_suspects = {addr for addr in filtered if _is_suspect_email(addr)}
    suspects_set: Set[str] = set(meta_suspects) | heuristics_suspects
    filtered_set_initial = set(filtered)
    suspects_removed = suspects_set & filtered_set_initial
    if SUSPECTS_REQUIRE_CONFIRM and suspects_removed:
        filtered = [addr for addr in filtered if addr not in suspects_removed]
    filtered = sorted(filtered)
    filtered_set_final = set(filtered)

    dropped_current: List[Tuple[str, str]] = []
    for email in sorted(allowed_all):
        if email in filtered_set_final:
            continue
        if email in technical_emails:
            dropped_current.append((email, "technical-address"))
        elif not is_allowed_tld(email):
            dropped_current.append((email, "foreign-domain"))
        else:
            dropped_current.append((email, "filtered"))

    suspicious_items = sorted(suspicious_map.items())
    for candidate, reason in suspicious_items:
        if reason == "suspect" and not SUSPECTS_REQUIRE_CONFIRM:
            continue
        dropped_current.append((candidate, reason))
    if SUSPECTS_REQUIRE_CONFIRM and suspects_removed:
        existing_suspects = {
            addr for addr, reason in suspicious_items if reason == "suspect"
        }
        heuristics_only = sorted(suspects_removed - existing_suspects)
        for addr in heuristics_only:
            dropped_current.append((addr, "suspect"))

    all_found = {addr for addr in allowed_all | loose_meta if addr}
    preview_stats = {
        "suspicious_count": len(suspicious_map),
        "foreign_domains": len(foreign_all),
        "role_rejected": role_rejected_total,
    }
    preview_message: str | None = None
    if (
        all_found
        or filtered
        or preview_stats["suspicious_count"]
        or preview_stats["role_rejected"]
    ):
        preview_message = _format_preview_text(
            all_found,
            filtered,
            dropped_current,
            preview_stats,
        )

    state = get_state(context)
    state.all_emails.update(allowed_all)
    current = set(state.to_send)
    current.update(filtered)
    state.to_send = _drop_truncated_twins(sorted(current), state=state)
    context.user_data["emails_for_sending"] = list(state.to_send)
    foreign_total = set(state.foreign) | set(foreign_all)
    state.repairs = list(dict.fromkeys((state.repairs or []) + repairs_all))
    state.repairs_sample = sample_preview(
        [f"{b} → {g}" for (b, g) in state.repairs], 6
    )
    total_footnote = state.footnote_dupes + footnote_dupes

    existing = list(state.dropped or [])
    combined_map: dict[str, str] = {}
    for addr, reason in existing + dropped_current:
        if addr not in combined_map:
            combined_map[addr] = reason
    dropped_total = [(addr, combined_map[addr]) for addr in combined_map]

    try:
        cooldown_blocked = [
            addr
            for addr in sorted(
                {
                    str(candidate).strip().lower()
                    for candidate in state.all_emails
                    if isinstance(candidate, str) and "@" in candidate
                }
            )
            if messaging._should_skip_by_history(addr)[0]
        ]
    except Exception:
        cooldown_blocked = []

    report = await _compose_report_and_save(
        context,
        state.all_emails,
        state.to_send,
        dropped_total,
        sorted(foreign_total),
        total_footnote,
        cooldown_blocked,
    )
    if SUSPECTS_REQUIRE_CONFIRM and suspects_removed:
        context.user_data["emails_suspects"] = sorted(suspects_removed)
    else:
        context.user_data["emails_suspects"] = []
    context.user_data["await_edit_suspects"] = False
    if state.repairs_sample:
        report += "\n\n🧩 Возможные исправления (проверьте вручную):"
        for s in state.repairs_sample:
            report += f"\n{s}"
    preview = context.chat_data.get("send_preview", {})
    dropped_preview = preview.get("dropped", [])
    extra_buttons: List[List[InlineKeyboardButton]] = []
    if C.ALLOW_EDIT_AT_PREVIEW:
        fix_buttons: List[InlineKeyboardButton] = []
        for idx in range(min(len(dropped_preview), 5)):
            fix_buttons.append(
                InlineKeyboardButton(
                    f"✏️ Исправить №{idx + 1}", callback_data=f"fix:{idx}"
                )
            )
        if fix_buttons:
            extra_buttons.append(fix_buttons)
    extra_buttons.append(
        [
            InlineKeyboardButton(
                "🔁 Показать ещё примеры", callback_data="refresh_preview"
            )
        ]
    )
    suspects_preview = [
        addr for addr, reason in dropped_preview if reason == "suspect"
    ]
    if SUSPECTS_REQUIRE_CONFIRM and suspects_preview:
        extra_buttons.append(
            [
                InlineKeyboardButton(
                    "✅ Принять подозрительные",
                    callback_data="accept_suspects",
                ),
                InlineKeyboardButton(
                    "✍️ Исправить адреса", callback_data="edit_suspects"
                ),
            ]
        )
    if state.repairs:
        extra_buttons.append(
            [
                InlineKeyboardButton(
                    f"🧩 Применить исправления ({len(state.repairs)})",
                    callback_data="apply_repairs",
                )
            ]
        )
        extra_buttons.append(
            [
                InlineKeyboardButton(
                    "🧩 Показать все исправления", callback_data="show_repairs"
                )
            ]
        )
    extra_buttons.append(
        [
            InlineKeyboardButton(
                "▶️ Перейти к выбору направления", callback_data="proceed_group"
            )
        ]
    )
    report += "\n\nДополнительные действия:"

    visited = max(visited, total_pages)
    last_from_stats = None
    for stats in stats_sequence:
        last_candidate = stats.get("last_url") if isinstance(stats, dict) else None
        if last_candidate:
            last_from_stats = last_candidate
    final_last = last_from_stats or last_url_seen or clean_url
    final_found = len(all_found)
    final_text = (
        f"Готово. Найдено адресов: {final_found}\n"
        f"Просмотрено страниц: {visited}\n"
        f"{filters_line}"
        f"Последняя: {final_last}"
    )
    if status_message_id is not None and status_chat_id is not None:
        try:
            await context.bot.edit_message_text(
                chat_id=status_chat_id,
                message_id=status_message_id,
                text=final_text,
            )
        except Exception:
            pass
    elif message:
        await _safe_reply_text(message, final_text)

    if message:
        await _safe_reply_text(
            message,
            report,
            reply_markup=InlineKeyboardMarkup(extra_buttons),
        )
        if preview_message:
            await _safe_reply_text(message, preview_message)


async def show_foreign_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show e-mail addresses with foreign domains."""

    query = update.callback_query
    state = context.chat_data.get(SESSION_KEY)
    foreign = state.foreign if state else []
    if not foreign:
        await query.answer("Список пуст", show_alert=True)
        return
    await query.answer()
    for chunk in _chunk_list(foreign, 60):
        await _safe_reply_text(query.message, "🌍 Иностранные домены:\n" + "\n".join(chunk))


async def apply_repairs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Apply suggested address repairs to the send list."""

    query = update.callback_query
    state = get_state(context)
    repairs: List[tuple[str, str]] = state.repairs
    if not repairs:
        await query.answer("Нет кандидатов на исправление", show_alert=True)
        return
    await query.answer()
    current = set(state.to_send)
    applied = 0
    changed = []
    for bad, good in repairs:
        if bad in current:
            current.discard(bad)
            if is_allowed_tld(good):
                current.add(good)
                applied += 1
                if applied <= 12:
                    changed.append(f"{bad} → {good}")
    state.to_send = _drop_truncated_twins(sorted(current), state=state)
    state.preview_allowed_all = _drop_truncated_twins(
        state.preview_allowed_all, update_counter=False
    )
    txt = f"🧩 Применено исправлений: {applied}."
    if changed:
        txt += "\n" + "\n".join(changed)
        if applied > len(changed):
            txt += f"\n… и ещё {applied - len(changed)}."
    await _safe_reply_text(query.message, txt)


async def show_repairs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display all potential e-mail address repairs."""

    query = update.callback_query
    state = context.chat_data.get(SESSION_KEY)
    repairs: List[tuple[str, str]] = state.repairs if state else []
    if not repairs:
        await query.answer("Список пуст", show_alert=True)
        return
    await query.answer()
    pairs = [f"{b} → {g}" for (b, g) in repairs]
    for chunk in _chunk_list(pairs, 60):
        await _safe_reply_text(query.message, "🧩 Возможные исправления:\n" + "\n".join(chunk))


async def manual_ignore_selected(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Allow selecting individual addresses to ignore the cooldown."""

    query = update.callback_query
    data = query.data or ""
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "go"
    argument = parts[2] if len(parts) > 2 else None

    candidates = _manual_override_candidates(context)

    if action == "go":
        if not candidates:
            await query.answer("Нет адресов для игнорирования", show_alert=True)
            return
        await query.answer()
        await _manual_override_show(query, context, 0)
        return

    if action == "close":
        selected = _manual_override_selected_set(context)
        _manual_override_store_selected(context, selected)
        days_raw = context.chat_data.get("manual_override_days")
        days = days_raw if isinstance(days_raw, int) and days_raw > 0 else None
        count = len(_manual_override_selected_set(context))
        if count and days:
            summary = (
                f"Игнорирование правила {days} дней для {count} адресов сохранено."
            )
        elif count:
            summary = f"Игнорирование для {count} адресов сохранено."
        else:
            summary = "Игнорирование не выбрано."
        await query.answer()
        await query.message.edit_text(summary)
        return

    if not candidates:
        await query.answer("Список пуст", show_alert=True)
        return

    if action == "page":
        try:
            page = int(argument or "0")
        except (TypeError, ValueError):
            await query.answer("Некорректная страница", show_alert=True)
            return
        await query.answer()
        await _manual_override_show(query, context, page, edit=True)
        return

    if action == "toggle":
        try:
            idx = int(argument or "-1")
        except (TypeError, ValueError):
            await query.answer("Некорректный индекс", show_alert=True)
            return
        if idx < 0 or idx >= len(candidates):
            await query.answer("Индекс вне диапазона", show_alert=True)
            return
        selected = _manual_override_selected_set(context)
        email_addr = candidates[idx]["email"]
        if email_addr in selected:
            selected.remove(email_addr)
        else:
            selected.add(email_addr)
        _manual_override_store_selected(context, selected)
        await query.answer()
        page = _manual_override_current_page(context)
        await _manual_override_show(query, context, page, edit=True)
        return

    if action == "clear":
        _manual_override_store_selected(context, set())
        await query.answer("Выбор очищен")
        page = _manual_override_current_page(context)
        await _manual_override_show(query, context, page, edit=True)
        return

    if action == "apply":
        selected = _manual_override_selected_set(context)
        _manual_override_store_selected(context, selected)
        count = len(_manual_override_selected_set(context))
        await query.answer(f"Игнорируем: {count}")
        page = _manual_override_current_page(context)
        await _manual_override_show(query, context, page, edit=True)
        return

    await query.answer()


async def manual_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сброс состояния ручной рассылки."""

    query = update.callback_query
    await query.answer()
    clear_all_awaiting(context)
    init_state(context)
    context.chat_data.pop("manual_selected_template_code", None)
    context.chat_data.pop("manual_selected_template_label", None)
    context.chat_data.pop("manual_selected_emails", None)
    context.chat_data.pop("manual_drop_reasons", None)
    _manual_override_clear(context)
    await _safe_reply_text(query.message,
        "Сброшено. Нажмите /manual для новой ручной рассылки."
    )


async def send_manual_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send e-mails entered manually by the user."""

    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    emails = context.chat_data.get("manual_all_emails") or []
    mode = context.chat_data.get("manual_send_mode", "allowed")
    override_active = mode == "all"
    override_selected = {
        addr for addr in _manual_override_selected_set(context) if addr in emails
    }
    _manual_override_store_selected(context, override_selected)
    data = query.data or ""
    if ":" not in data:
        await _safe_reply_text(query.message, 
            "⚠️ Некорректный выбор шаблона. Обновите список и попробуйте снова."
        )
        return
    prefix_raw, group_raw = data.split(":", 1)
    prefix = f"{prefix_raw}:"
    template_info = get_template_from_map(context, prefix, group_raw)
    template_path_obj = _template_path(template_info)
    if not template_info or not template_path_obj or not template_path_obj.exists():
        group_code_fallback = _normalize_template_code(group_raw)
        template_info = get_template(group_code_fallback)
        template_path_obj = _template_path(template_info)
        if not template_info or not template_path_obj or not template_path_obj.exists():
            await _safe_reply_text(query.message, 
                "⚠️ Шаблон не найден или файл отсутствует. Обновите список и попробуйте снова."
            )
            return
        group_raw = template_info.get("code") or group_code_fallback
    group_code = _normalize_template_code(group_raw)
    template_path = str(template_path_obj)
    label = _template_label(template_info)
    if not label and group_code:
        label = get_template_label(group_code)
    if not label:
        label = group_code

    enforce, days, allow_override = _manual_cfg()
    rule_days = days if isinstance(days, int) and days > 0 else 180
    override_to_send: list[str] = []
    if enforce and mode == "allowed":
        allowed, rejected_all = _filter_by_180(
            list(emails), group_code, days, chat_id=chat_id
        )
        override_to_send = [
            addr for addr in rejected_all if addr in override_selected
        ]
        rejected = [addr for addr in rejected_all if addr not in override_selected]
        to_send = allowed + override_to_send
        _manual_override_store_selected(context, set(override_to_send))
    else:
        to_send = list(emails)
        rejected = []
        if not override_active:
            _manual_override_store_selected(context, set())

    blocked_manual: list[str] = []
    filtered_to_send: list[str] = []
    block_set = {normalize_email(item) for item in rules.load_blocklist() if item}
    for email_addr in to_send:
        norm = normalize_email(email_addr)
        if norm and norm in block_set:
            blocked_manual.append(email_addr)
        else:
            filtered_to_send.append(email_addr)
    to_send = filtered_to_send

    # Если вообще нет исходных адресов — подскажем и выйдем
    if not emails:
        await _safe_reply_text(query.message, 
            "Список адресов пуст. Нажмите /manual и введите адреса."
        )
        return

    # Сообщение без раскрытия адресов — только счётчики
    display_label = label
    if label.lower() != group_code:
        display_label = f"{label} ({group_code})"
    lines = [f"Шаблон: {display_label}", f"К отправке: {len(to_send)}"]
    if rejected:
        lines.append(
            f"Отфильтровано по правилу {rule_days} дней: {len(rejected)}"
        )
    if override_active:
        lines.append(
            f"Правило {rule_days} дней будет проигнорировано для всех адресов."
        )
    elif override_to_send:
        lines.append(
            (
                "Правило {days} дней будет проигнорировано для: "
                "{count} адресов"
            ).format(days=rule_days, count=len(override_to_send))
        )
    if blocked_manual:
        lines.append(f"Исключено по блок-листу: {len(blocked_manual)}")
    await _safe_reply_text(query.message, "\n".join(lines))

    # Если отправлять нечего (всё отфильтровано) — не запускаем рассылку
    if len(to_send) == 0:
        if allow_override and len(rejected) > 0:
            await _safe_reply_text(query.message,
                f"Все адреса были отфильтрованы правилом {rule_days} дней.\n"
                f"Вы можете нажать «Отправить всем» для игнорирования правила {rule_days} дней."
            )
        else:
            reasons: list[str] = []
            if rejected:
                reasons.append("правилом 180 дней")
            if blocked_manual:
                reasons.append("блок-листом")
            reason_txt = " и ".join(reasons) if reasons else "правилами отправки"
            await _safe_reply_text(query.message, 
                f"Все адреса были отфильтрованы {reason_txt}. Отправка не запущена."
            )
        return

    state = get_state(context)
    to_send = _drop_truncated_twins(to_send, state=state)
    if override_active:
        override_set: set[str] = set(to_send)
    else:
        override_set = {addr for addr in override_to_send if addr in to_send}
        _manual_override_store_selected(context, override_set)
    # Сохраняем выбранный набор; дальнейшая логика подхватит эти значения
    context.chat_data["manual_selected_template_code"] = group_code
    context.chat_data["manual_selected_template_label"] = label
    context.chat_data["manual_selected_emails"] = to_send

    await notify(query.message, "Запущено — выполняю в фоне...", event="progress")

    async def long_job() -> None:
        chat_id = query.message.chat.id

        # manual отправка не учитывает супресс-лист
        get_blocked_emails()
        sent_today = get_sent_today()
        preview = context.chat_data.get("send_preview", {}) or {}
        fixed_map: Dict[str, str] = {}
        for item in preview.get("fixed", []):
            if isinstance(item, dict):
                new_addr = item.get("to")
                original_addr = item.get("from")
                if new_addr and original_addr:
                    fixed_map[str(new_addr)] = str(original_addr)

        try:
            IMAP_HOST = os.getenv("IMAP_HOST", "imap.mail.ru")
            IMAP_TIMEOUT = float(os.getenv("IMAP_TIMEOUT", "15"))
            imap = imaplib.IMAP4_SSL(IMAP_HOST, timeout=IMAP_TIMEOUT)
            imap.login(messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD)
            sent_folder = get_preferred_sent_folder(imap)
            imap.select(f'"{sent_folder}"')
        except Exception as e:
            log_error(f"imap connect: {e}")
            await notify(query.message, f"❌ IMAP ошибка: {e}", event="error")
            return

        state_snapshot = get_state(context)
        to_send_local = _drop_truncated_twins(
            list(to_send), state=state_snapshot
        )
        override_set_local = {addr for addr in override_set if addr in to_send_local}

        available = max(0, MAX_EMAILS_PER_DAY - len(sent_today))
        if available <= 0 and not is_force_send(chat_id):
            logger.info(
                "Daily limit reached: %s emails sent today (source=sent_log)",
                len(sent_today),
            )
            await notify(
                update.callback_query.message,
                (
                    f"❗ Дневной лимит {MAX_EMAILS_PER_DAY} уже исчерпан.\n"
                    "Если вы исправили ошибки — нажмите "
                    "«🚀 Игнорировать лимит» и запустите ещё раз."
                ),
                event="error",
            )
            return
        if not is_force_send(chat_id) and len(to_send_local) > available:
            to_send_local = to_send_local[:available]
            override_set_local &= set(to_send_local)
            await notify(
                query.message,
                (
                    f"⚠️ Учитываю дневной лимит: будет отправлено "
                    f"{available} адресов из списка."
                ),
                event="progress",
            )

        await notify(
            query.message,
            f"✉️ Рассылка начата. Отправляем {len(to_send_local)} писем...",
            event="start",
        )

        batch_id = context.chat_data.get("batch_id")
        audit_path = Path("var") / f"bulk_audit_{batch_id or int(time.time())}.jsonl"
        try:
            audit_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            logger.debug("bulk audit mkdir failed", exc_info=True)

        def _audit(email: str, status: str, detail: str = "") -> None:
            try:
                with audit_path.open("a", encoding="utf-8") as f:
                    f.write(
                        json.dumps(
                            {
                                "email": email,
                                "status": status,
                                "detail": detail,
                                "ts": time.time(),
                            }
                        )
                        + "\n"
                    )
            except Exception:
                logger.debug("bulk audit append failed", exc_info=True)

        sent_ok: list[str] = []
        skipped_recent: list[str] = []
        blocked_recipients: list[str] = []
        error_addresses: list[str] = []
        errors: list[str] = []
        cancel_event = context.chat_data.get("cancel_event")
        smtp = RobustSMTP()
        try:
            for email_addr in to_send_local:
                if cancel_event and cancel_event.is_set():
                    break
                try:
                    outcome, token = send_email_with_sessions(
                        smtp,
                        imap,
                        sent_folder,
                        email_addr,
                        template_path,
                        fixed_from=fixed_map.get(email_addr),
                        group_title=label,
                        group_key=group_code,
                        override_180d=email_addr in override_set_local,
                    )
                    if outcome == SendOutcome.SENT:
                        log_sent_email(
                            email_addr,
                            group_code,
                            "ok",
                            chat_id,
                            template_path,
                            unsubscribe_token=token,
                        )
                        sent_ok.append(email_addr)
                        _audit(email_addr, "sent")
                        await asyncio.sleep(1.5)
                    elif outcome == SendOutcome.COOLDOWN:
                        if email_addr not in skipped_recent:
                            skipped_recent.append(email_addr)
                        _audit(email_addr, "cooldown")
                    elif outcome == SendOutcome.BLOCKED:
                        if email_addr not in blocked_recipients:
                            blocked_recipients.append(email_addr)
                        _audit(email_addr, "blocked")
                    else:
                        if email_addr not in error_addresses:
                            error_addresses.append(email_addr)
                        errors.append(f"{email_addr} — outcome {outcome}")
                        _audit(email_addr, "error", f"outcome {outcome}")
                except smtplib.SMTPResponseException as e:
                    code = int(getattr(e, "smtp_code", 0) or 0)
                    raw = getattr(e, "smtp_error", b"") or b""
                    if isinstance(raw, (bytes, bytearray)):
                        msg = raw.decode("utf-8", "ignore")
                    else:
                        msg = str(raw)
                    detail = f"{code} {msg}".strip()
                    errors.append(f"{email_addr} — {detail}")
                    add_bounce(email_addr, code, msg, phase="send")
                    target_list = (
                        blocked_recipients
                        if is_hard_bounce(code, msg)
                        else error_addresses
                    )
                    if email_addr not in target_list:
                        target_list.append(email_addr)
                    _audit(email_addr, "error", detail)
                except Exception as e:
                    errors.append(f"{email_addr} — {e}")
                    code = None
                    msg = None
                    if (
                        hasattr(e, "recipients")
                        and isinstance(e.recipients, dict)
                        and email_addr in e.recipients
                    ):
                        code, msg = (
                            e.recipients[email_addr][0],
                            e.recipients[email_addr][1],
                        )
                    elif hasattr(e, "smtp_code"):
                        code = getattr(e, "smtp_code", None)
                        msg = getattr(e, "smtp_error", None)
                    add_bounce(email_addr, code, str(msg or e), phase="send")
                    if email_addr not in error_addresses:
                        error_addresses.append(email_addr)
                    _audit(email_addr, "error", str(e))
        finally:
            smtp.close()
        imap.logout()
        summary_text = build_mass_report_text(
            sent_ok,
            skipped_recent,
            None,
            blocked_recipients,
        )
        if not skipped_recent:
            summary_text = summary_text.replace(
                "\n⏳ Пропущены (<180 дней/идемпотентность): 0", ""
            )
            if summary_text.startswith("⏳ Пропущены (<180 дней/идемпотентность): 0\n"):
                summary_text = summary_text.split("\n", 1)[-1]
        if "🚫 В блок-листе/недоступны: 0" in summary_text:
            summary_text = summary_text.replace(
                "\n🚫 В блок-листе/недоступны: 0", ""
            )
            if summary_text.startswith("🚫 В блок-листе/недоступны: 0\n"):
                summary_text = summary_text.split("\n", 1)[-1]
        if error_addresses:
            summary_text = (
                f"{summary_text}\n❌ Ошибок при отправке: {len(error_addresses)}"
                if summary_text
                else f"❌ Ошибок при отправке: {len(error_addresses)}"
            )
        if audit_path:
            summary_text = f"{summary_text}\n\n📄 Аудит: {audit_path}"
        if cancel_event and cancel_event.is_set():
            summary_text = f"🛑 Остановлено.\n{summary_text}"
        await notify(query.message, summary_text, event="finish")
        if errors:
            await notify(
                query.message,
                "Ошибки:\n" + "\n".join(errors),
                event="error",
            )

        context.chat_data["manual_all_emails"] = []
        _manual_override_clear(context)
        clear_recent_sent_cache()
        disable_force_send(chat_id)

    messaging.create_task_with_logging(
        long_job(), functools.partial(notify, query.message, event="error")
    )


async def autosync_imap_with_message(query: CallbackQuery) -> None:
    """Synchronize IMAP logs and notify the user via message."""
    await query.answer()
    await notify(
        query.message,
        "🔄 Синхронизация истории отправки с сервером...",
        event="analysis",
        force=True,
    )
    loop = asyncio.get_running_loop()
    stats = await loop.run_in_executor(None, sync_log_with_imap)
    clear_recent_sent_cache()
    await notify(
        query.message,
        "✅ Синхронизация завершена. "
        f"новых: {stats['new_contacts']}, обновлено: {stats['updated_contacts']}, "
        f"пропущено: {stats['skipped_events']}, всего: {stats['total_rows_after']}.\n"
        f"История отправки обновлена на последние 6 месяцев.",
        event="analysis",
        force=True,
    )


def _chunk_list(items: List[str], size: int = 60) -> List[List[str]]:
    """Split ``items`` into chunks of ``size`` elements."""

    return [items[i : i + size] for i in range(0, len(items), size)]


__all__ = [
    "start",
    "prompt_upload",
    "about_bot",
    "add_block_prompt",
    "show_blocked_list",
    "prompt_change_group",
    "force_send_command",
    "report_command",
    "report_callback",
    "sync_imap_command",
    "sync_bounces_command",
    "reset_email_list",
    "diag",
    "dedupe_log_command",
    "handle_document",
    "refresh_preview",
    "parse_mode_cb",
    "proceed_to_group",
    "select_group",
    "prompt_manual_email",
    "handle_text",
    "page_url_command",
    "request_fix",
    "show_foreign_list",
    "apply_repairs",
    "show_repairs",
    "manual_mode",
    "manual_reset",
    "send_manual_email",
    "send_all",
    "autosync_imap_with_message",
]
