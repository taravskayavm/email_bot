"""Telegram bot handlers."""

from __future__ import annotations

import asyncio
import csv
from collections import Counter
import imaplib
import importlib
import inspect
import io
import itertools
import threading
import json
import logging
import math
import os
import re
import secrets
import time
import urllib.parse
import uuid
import calendar
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

from . import report_service
from . import send_selected as _pkg_send_selected
from .extraction_zip import ZIP_MAX_DEPTH, ZIP_MAX_FILES, ZIP_MAX_TOTAL_UNCOMP_MB
from .time_utils import LOCAL_TZ, day_bounds, parse_ts, parse_user_date_once
from zoneinfo import ZoneInfo

from .utils.zip_limits import validate_zip_safely
from .worker_archive import run_parse_in_subprocess

logger = logging.getLogger(__name__)


# Последний счётчик записей без timestamp, отфильтрованных при чтении аудита.
_LAST_AUDIT_DROP_NO_TS = 0

# Кэш массового отправителя. Инициализируем лениво, чтобы не ловить циклический импорт.
_LEGACY_MASS_SENDER: Optional[Callable] = None
# [EBOT-073] Копим описание ошибок импорта, чтобы показать пользователю первопричину.
_LEGACY_MASS_SENDER_ERR: Optional[str] = None


def _resolve_mass_handler() -> Optional[Callable]:
    """Вернуть обработчик массовой рассылки, если он доступен."""

    global _LEGACY_MASS_SENDER, _LEGACY_MASS_SENDER_ERR

    handler = globals().get("send_selected")
    if callable(handler):
        _LEGACY_MASS_SENDER_ERR = None
        logger.info("start_sending: using handler=send_selected (globals)")
        return handler

    if callable(_pkg_send_selected):
        _LEGACY_MASS_SENDER_ERR = None
        logger.info("start_sending: using handler=send_selected (package export)")
        return _pkg_send_selected

    if _LEGACY_MASS_SENDER is None:
        _LEGACY_MASS_SENDER = _import_mass_sender()

    if callable(_LEGACY_MASS_SENDER):
        logger.info("start_sending: using handler=manual_send.send_all (lazy)")
        return _LEGACY_MASS_SENDER

    return None


def _import_mass_sender() -> Optional[Callable]:
    """Попробовать импортировать manual_send.send_all несколькими путями."""

    global _LEGACY_MASS_SENDER, _LEGACY_MASS_SENDER_ERR
    errors: list[str] = []

    # 1) Относительный импорт внутри пакета
    try:
        from .handlers.manual_send import send_all as _fn  # type: ignore

        _LEGACY_MASS_SENDER = _fn
        _LEGACY_MASS_SENDER_ERR = None
        logger.info("start_sending: using handler=.handlers.manual_send.send_all")
        return _fn
    except Exception as e1:  # pragma: no cover - defensive
        logger.debug("mass_sender import (relative) failed: %r", e1)
        errors.append(f"relative: {e1!r}")

    # 2) Абсолютный импорт пакетного модуля
    try:
        module = importlib.import_module("emailbot.handlers.manual_send")
        fn = getattr(module, "send_all", None)
        if callable(fn):
            _LEGACY_MASS_SENDER = fn
            _LEGACY_MASS_SENDER_ERR = None
            logger.info(
                "start_sending: using handler=emailbot.handlers.manual_send.send_all"
            )
            return fn
        errors.append("emailbot.handlers.manual_send: send_all not callable/absent")
    except Exception as e2:  # pragma: no cover - defensive
        logger.debug("mass_sender import (emailbot.*) failed: %r", e2)
        errors.append(f"emailbot.handlers.manual_send: {e2!r}")

    _LEGACY_MASS_SENDER = None
    _LEGACY_MASS_SENDER_ERR = " | ".join(errors) if errors else "unknown"
    return None

import aiohttp
import httpx
import pandas as pd
from telegram import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    Message,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.error import BadRequest
from telegram.ext import ApplicationHandlerStop, ContextTypes, ConversationHandler

BASE_DIR = Path(__file__).resolve().parent
# Каталог для загрузок по умолчанию (рядом с логами/вар):
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR") or str(
    (BASE_DIR / ".." / "var" / "uploads").resolve()
)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

SUPPORTED_DOCUMENT_EXTENSIONS = {
    ".zip",
    ".pdf",
    ".doc",
    ".docx",
    ".xlsx",
    ".xls",
    ".csv",
    ".txt",
}

ZIP_JOB_TIMEOUT_SEC = int(os.getenv("ZIP_JOB_TIMEOUT_SEC", "600"))
ZIP_HEARTBEAT_SEC = int(os.getenv("ZIP_HEARTBEAT_SEC", "12"))
try:
    ZIP_HEARTBEAT_MIN_SEC = max(1.0, float(os.getenv("ZIP_HEARTBEAT_MIN_SEC", "5")))
except ValueError:
    ZIP_HEARTBEAT_MIN_SEC = 5.0

_PROGRESS_EDIT_LOCK = asyncio.Lock()

EXCLUDE_GLOBAL_MAIL = str(os.getenv("EXCLUDE_GLOBAL_MAIL", "0")).lower() in {
    "1",
    "true",
    "yes",
}


# Conversation state identifiers
BULK_DELETE = 101


def _build_parse_task_name(update: Update, mode: str) -> str:
    """Compose a unique task name for long-running parsing jobs."""

    effective_user = getattr(update, "effective_user", None)
    effective_chat = getattr(update, "effective_chat", None)
    user_id = getattr(effective_user, "id", None)
    chat_id = getattr(effective_chat, "id", None)
    base = user_id or chat_id or "anon"
    return f"parse:{base}:{mode}"

# Простая регулярка для поиска ссылок в пользовательском тексте
URL_REGEX = re.compile(r"https?://[^\s<>]+", re.IGNORECASE)
# Более строгий детектор URL для ручного режима: распознаём http(s) и www.
_MANUAL_URL_RE = re.compile(r"(?i)(?<!@)\bwww\.")


def _message_has_url(message: Message | None, raw_text: str | None) -> bool:
    """Return ``True`` if ``raw_text`` or entities contain a URL-like token."""

    text = raw_text or ""
    entities = getattr(message, "entities", None)
    if entities:
        for ent in entities:
            if ent.type == "url":
                return True
            if ent.type == "text_link" and getattr(ent, "url", None):
                return True
    if not text:
        return False
    lowered = text.lower()
    if "http://" in lowered or "https://" in lowered:
        return True
    if URL_REGEX.search(text):
        return True
    if _MANUAL_URL_RE.search(text):
        return True
    return False

EMAIL_CORE = r"[A-Za-z0-9._%+\-]{1,64}@[A-Za-z0-9.\-]{1,255}\.[A-Za-z]{2,24}"
EMAIL_ANYWHERE_RE = re.compile(EMAIL_CORE)

_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


def _normalize_email(value: str) -> str:
    return (value or "").strip().lower().lstrip(".")


def _head3(arr: list[str]) -> str:
    return ", ".join(arr[:3]) if arr else "—"

# Состояние для пользовательских запросов отчётов.
REPORT_STATE: dict[int, dict[str, object]] = {}


def _report_menu_kb() -> InlineKeyboardMarkup:
    """Построить клавиатуру выбора периода отчёта."""

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📆 День", callback_data="report:day"),
                InlineKeyboardButton("🗓 Неделя", callback_data="report:week"),
            ],
            [
                InlineKeyboardButton("📆 Месяц", callback_data="report:month"),
                InlineKeyboardButton("📈 Год", callback_data="report:year"),
            ],
            [InlineKeyboardButton("📌 День по дате…", callback_data="report:single")],
        ]
    )


def _confirm_period_kb() -> InlineKeyboardMarkup:
    """Клавиатура подтверждения периода отчёта."""

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Подтвердить", callback_data="report:confirm"),
                InlineKeyboardButton("❌ Отмена", callback_data="report:cancel"),
            ]
        ]
    )


def _format_period_label(start_iso: str, end_iso: str) -> str:
    """Вернуть человеко-читаемое описание периода."""

    try:
        start_dt = datetime.strptime(start_iso, "%Y-%m-%d")
        end_dt = datetime.strptime(end_iso, "%Y-%m-%d")
    except ValueError:
        return f"{start_iso} — {end_iso}" if start_iso != end_iso else start_iso

    start_label = start_dt.strftime("%d.%m.%Y")
    end_label = end_dt.strftime("%d.%m.%Y")
    return start_label if start_iso == end_iso else f"{start_label} — {end_label}"


_DATE_TOKEN_RE = re.compile(r"\b(?:\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4})\b")


def _extract_emails_loose(text: str) -> list[str]:
    """Return unique e-mail addresses extracted from ``text``."""

    if not text:
        return []
    seen: set[str] = set()
    found: list[str] = []
    for match in EMAIL_ANYWHERE_RE.finditer(text):
        email = match.group(0)
        if email not in seen:
            seen.add(email)
            found.append(email)
    return found

from emailbot.domain_utils import count_domains, classify_email_domain
from emailbot.reporting.excel_helpers import append_foreign_review_sheet
from emailbot.ui.keyboards import (
    build_after_parse_combined_kb,
    build_bulk_edit_kb,
    build_skipped_preview_entry_kb,
    build_skipped_preview_kb,
    groups_map,
)
from emailbot.notify import notify
from emailbot.ui.messages import (
    format_dispatch_start,
    format_error_details,
    format_parse_summary,
)

from emailbot.config import ENABLE_INLINE_EMAIL_EDITOR
from emailbot.run_control import (
    clear_stop,
    register_task,
    should_stop,
    stop_and_status,
    unregister_task,
)

from . import messaging
from . import messaging_utils as mu
from . import extraction as _extraction
from . import extraction_pdf as _pdf
from .extraction import normalize_email, smart_extract_emails, extract_emails_manual
from .progress_watchdog import heartbeat, start_watchdog, start_heartbeat_pulse
from .reporting import log_mass_filter_digest, count_blocked
from . import settings
from . import mass_state
from .session_store import load_last_summary, save_last_summary
from .settings import REPORT_TZ, SKIPPED_PREVIEW_LIMIT
from .settings_store import DEFAULTS
from emailbot.cooldown import (
    CooldownHit,
    CooldownService,
    audit_emails as cooldown_audit_emails,
    build_cooldown_service,
    normalize_email as cooldown_normalize_email,
)
from emailbot.web_extract import fetch_and_extract
from pipelines.extract_emails import extract_from_url_async as deep_extract_async
from emailbot.suppress_list import is_blocked
from .imap_reconcile import reconcile_csv_vs_imap, build_summary_text, to_csv_bytes
from .selfcheck import format_checks as format_selfcheck, run_selfcheck

from utils.email_clean import sanitize_email
from emailbot.services.cooldown import (
    COOLDOWN_WINDOW_DAYS,
    check_email,
    should_skip_by_cooldown,
)
from services.templates import get_template, get_template_label, list_templates


def _preclean_text_for_emails(text: str) -> str:
    return text


def apply_numeric_truncation_removal(allowed):
    return allowed, []


def _watchdog_idle_seconds() -> float:
    stalled_raw = (os.getenv("WATCHDOG_STALLED_MS", "") or "").strip()
    if stalled_raw:
        try:
            value = float(stalled_raw)
            if value > 0:
                return value / 1000.0
        except Exception:
            pass
    raw = (os.getenv("WD_IDLE_SECONDS", "") or "").strip()
    try:
        return float(raw) if raw else 90.0
    except Exception:
        return 90.0


def _snapshot_mass_digest(
    digest: dict[str, object] | None,
    *,
    ready_after_cooldown: int | None = None,
    ready_final: int | None = None,
) -> dict[str, int]:
    base = dict(digest or {})

    def _as_int(value: object, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    ready_after = (
        ready_after_cooldown
        if ready_after_cooldown is not None
        else _as_int(
            base.get("ready_after_cooldown")
            or base.get("after_180d")
            or base.get("ready")
            or base.get("sent_planned")
            or base.get("unique_ready_to_send"),
            0,
        )
    )
    ready_final_value = (
        ready_final
        if ready_final is not None
        else _as_int(
            base.get("ready_final")
            or base.get("sent_planned")
            or base.get("unique_ready_to_send"),
            ready_after,
        )
    )

    snapshot = {
        "ready_after_cooldown": ready_after,
        "removed_recent_180d": _as_int(
            base.get("removed_recent_180d")
            or base.get("skipped_recent")
            or base.get("skipped_180d"),
            0,
        ),
        "removed_today": _as_int(base.get("removed_today"), 0),
        "removed_invalid": _as_int(
            base.get("removed_invalid")
            or base.get("blocked_invalid")
            or base.get("skipped_suppress"),
            0,
        ),
        "removed_foreign": _as_int(
            base.get("removed_foreign")
            or base.get("blocked_foreign")
            or base.get("skipped_foreign"),
            0,
        ),
        "removed_duplicates_in_batch": _as_int(
            base.get("removed_duplicates_in_batch")
            or base.get("skipped_by_dup_in_batch"),
            0,
        ),
    }
    snapshot["set_planned"] = _as_int(base.get("set_planned"), ready_final_value)
    snapshot["ready_final"] = ready_final_value
    return snapshot


def _format_empty_send_explanation(digest: dict[str, object]) -> str:
    lines = [
        "<b>Все адреса сняты фильтрами — рассылать нечего.</b>",
        "",
        "Последний срез:",
        (
            "• 180-дневный период: снято {removed}, допущено {ready}".format(
                removed=int(digest.get("removed_recent_180d", 0)),
                ready=int(digest.get("ready_after_cooldown", 0)),
            )
        ),
        "• «Отправлены сегодня/24ч»: снято {count}".format(
            count=int(digest.get("removed_today", 0))
        ),
        "• Невалидные адреса: снято {count}".format(
            count=int(digest.get("removed_invalid", 0))
        ),
        "• Иностранные домены: снято {count}".format(
            count=int(digest.get("removed_foreign", 0))
        ),
        "• Дубликаты в батче: снято {count}".format(
            count=int(digest.get("removed_duplicates_in_batch", 0))
        ),
        "",
        "Откройте диагностические отчёты: <code>var/last_batch_digest.json</code> и <code>var/last_batch_examples.json</code>.",
        "Если хотите отправить несмотря на ограничение 180 дней — включите режим «Игнорировать лимит 180д».",
    ]
    return "\n".join(lines)


async def async_extract_emails_from_url(
    url: str, session, chat_id=None, batch_id: str | None = None
):
    if not settings.ENABLE_WEB:
        return url, set(), set(), [], {}

    await heartbeat()
    # Реализация использует пайплайн из emailbot.extraction через fetch_and_extract.
    final_url, emails = await fetch_and_extract(url)
    await heartbeat()
    foreign = {e for e in emails if not is_allowed_tld(e)}
    logger.info(
        "web extract complete",
        extra={"event": "web_extract", "source": final_url, "count": len(emails)},
    )
    return final_url, emails, foreign, [], {}


async def url_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Принудительный одностраничный разбор URL: /url <link>"""

    msg = update.message
    if not msg:
        return
    text = (msg.text or "").strip()
    if not text:
        return
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await msg.reply_text("Формат: /url <ссылка>")
        return

    url = parts[1].strip()
    if not URL_REGEX.search(url):
        await msg.reply_text("Не похоже на URL. Пример: /url https://example.com/page")
        return
    if not settings.ENABLE_WEB:
        await msg.reply_text("Веб-парсер отключён (ENABLE_WEB=0).")
        return

    lock = context.chat_data.setdefault("extract_lock", asyncio.Lock())
    if lock.locked():
        await msg.reply_text("⏳ Уже идёт анализ этого URL")
        return

    clear_stop()

    try:
        async with lock:
            final_url, emails, _foreign, _, _ = await async_extract_emails_from_url(
                url, context, chat_id=msg.chat_id
            )
    except httpx.HTTPStatusError as exc:
        await msg.reply_text(
            "Сайт ответил статусом "
            f"{exc.response.status_code} при загрузке страницы.\nПопробуй позже или другую ссылку."
        )
        return
    except httpx.ConnectError:
        await msg.reply_text(
            "Не удалось подключиться к сайту. Проверь ссылку или доступность ресурса."
        )
        return
    except httpx.ReadTimeout:
        await msg.reply_text(
            "Таймаут чтения страницы. Попробуй ещё раз или укажи другую ссылку."
        )
        return
    except Exception as exc:  # pragma: no cover - network/parse errors
        await msg.reply_text(f"Не удалось получить страницу: {type(exc).__name__}")
        return

    allowed = [e for e in sorted(emails) if not is_blocked(e)]

    if not allowed:
        await msg.reply_text("Адреса не найдены.")
        return

    await _send_emails_as_file(
        msg,
        allowed,
        source=final_url or url,
        title="Результат (1 страница)",
    )


async def crawl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Глубокий обход сайта: /crawl <url> [--max-pages N] [--max-depth D] [--prefix /staff,/contacts]"""

    msg = update.message
    if not msg:
        return
    if not settings.ENABLE_WEB:
        await msg.reply_text("Веб-парсер отключен (ENABLE_WEB=0). Включи в .env.")
        return

    raw = (msg.text or "").strip()
    if not raw:
        return

    parts = raw.split()
    if len(parts) < 2:
        await msg.reply_text(
            "Формат: /crawl <ссылка> [--max-pages N] [--max-depth D] [--prefix /path1,/path2]"
        )
        return

    url = parts[1]
    if not URL_REGEX.search(url):
        await msg.reply_text("Не похоже на ссылку. Пример: /crawl https://example.com")
        return

    max_pages: int | None = None
    max_depth: int | None = None
    prefixes: list[str] | None = None

    idx = 2
    while idx < len(parts):
        token = parts[idx]
        if token == "--max-pages" and idx + 1 < len(parts):
            try:
                max_pages = int(parts[idx + 1])
            except Exception:
                pass
            idx += 2
            continue
        if token == "--max-depth" and idx + 1 < len(parts):
            try:
                max_depth = int(parts[idx + 1])
            except Exception:
                pass
            idx += 2
            continue
        if token == "--prefix" and idx + 1 < len(parts):
            raw_prefixes = parts[idx + 1].split(",")
            prefixes = [p.strip() for p in raw_prefixes if p.strip()]
            idx += 2
            continue
        idx += 1

    last_report = {"ts": 0.0}

    def _progress(pages: int, page_url: str) -> None:
        now = time.time()
        if now - last_report["ts"] <= 2.5:
            return
        last_report["ts"] = now
        try:
            asyncio.create_task(
                msg.reply_text(f"Сканирую: {pages} стр. (посл.: {page_url})")
            )
        except Exception:
            pass

    clear_stop()

    try:
        emails, stats = await deep_extract_async(
            url,
            deep=True,
            progress_cb=_progress,
            path_prefixes=prefixes,
        )
    except Exception as exc:  # pragma: no cover - network/parse errors
        await msg.reply_text(f"Ошибка при обходе {url}: {exc.__class__.__name__}")
        return

    stats_map = stats if isinstance(stats, dict) else {}
    if max_pages is not None and isinstance(stats_map, dict):
        pages_val = stats_map.get("pages")
        if isinstance(pages_val, int) and pages_val > 0:
            stats_map["pages"] = min(pages_val, max_pages)
    if max_depth is not None and isinstance(stats_map, dict):
        stats_map["max_depth"] = max_depth
    if prefixes and isinstance(stats_map, dict):
        stats_map.setdefault("path_prefixes", prefixes)

    unique: list[str] = []
    seen: set[str] = set()
    for addr in emails:
        if addr in seen:
            continue
        seen.add(addr)
        if is_blocked(addr):
            continue
        unique.append(addr)

    if not unique:
        await msg.reply_text("Адреса не найдены.")
        return

    await _send_emails_as_file(
        msg,
        sorted(unique),
        source=url,
        title="Результат (глубокий обход)",
        stats=stats_map if isinstance(stats_map, dict) else None,
    )


def collapse_footnote_variants(emails):
    return emails


def collect_repairs_from_files(files):
    return []


class ZipValidationError(Exception):
    """Raised when a ZIP archive violates safety limits."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class ZipProcessingTimeoutError(Exception):
    """Raised when ZIP parsing exceeds the configured timeout."""


async def _edit_progress_message(progress_msg: Message | None, text: str) -> bool:
    """Try to update ``progress_msg`` text, ignoring minor Telegram errors."""

    if not progress_msg:
        return False

    async with _PROGRESS_EDIT_LOCK:
        try:
            await progress_msg.edit_text(text)
            return True
        except BadRequest as exc:
            message = str(getattr(exc, "message", exc)).lower()
            if "message is not modified" in message or "not found" in message:
                return True
        except Exception as exc:  # pragma: no cover - defensive log
            logger.debug("progress message update failed: %s", exc)
    return False


def _format_elapsed(seconds: float) -> str:
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes:02d}:{secs:02d}"


def _shorten_filename(name: str, *, limit: int = 32) -> str:
    base = os.path.basename(str(name).strip()) if name else ""
    if len(base) <= limit:
        return base
    if limit <= 1:
        return base[:limit]
    return base[: limit - 1] + "…"


async def _zip_status_heartbeat(
    progress_msg: Message | None,
    stop_event: asyncio.Event,
    *,
    started_at: float | None = None,
    progress_state: Dict[str, Any] | None = None,
    progress_lock: threading.Lock | None = None,
) -> None:
    """Periodically update ``progress_msg`` until ``stop_event`` is set."""

    # Если нет куда писать статус — нечего пульсировать
    if not progress_msg:
        return

    dots = itertools.cycle(["", ".", "..", "..."])
    base_period = float(ZIP_HEARTBEAT_SEC) if ZIP_HEARTBEAT_SEC > 0 else 12.0
    period = base_period
    t0 = started_at or time.monotonic()

    try:
        while True:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=max(period, 1.0))
                break
            except asyncio.TimeoutError:
                pass

            # 🔴 ВАЖНО: даём «пульс» сторожу, чтобы он не отменял задачу.
            # Во время долгого парсинга ZIP прогресс по файлам может идти редко,
            # а heartbeat обязан дёргаться регулярно.
            try:
                await heartbeat()
            except Exception:
                # пульс — best-effort, не мешаем основному циклу статуса
                pass
            elapsed = max(0.0, time.monotonic() - t0)
            suffix = next(dots)
            details: list[str] = []
            if progress_state is not None:
                if progress_lock is not None:
                    with progress_lock:
                        snapshot = dict(progress_state)
                else:
                    snapshot = dict(progress_state)
                processed_raw = snapshot.get("files_processed")
                total_raw = snapshot.get("files_total")
                try:
                    processed_val = int(processed_raw) if processed_raw is not None else None
                except (TypeError, ValueError):
                    processed_val = None
                try:
                    total_val = int(total_raw) if total_raw is not None else None
                except (TypeError, ValueError):
                    total_val = None
                if total_val and total_val > 0:
                    if processed_val is None:
                        processed_val = 0
                    processed_val = max(0, min(processed_val, total_val))
                    details.append(f"{processed_val}/{total_val}")
                elif processed_val and processed_val > 0:
                    details.append(str(processed_val))
                last_file = snapshot.get("last_file")
                if last_file:
                    details.append(_shorten_filename(last_file))
            suffix_text = f"{suffix} · {_format_elapsed(elapsed)}"
            if details:
                suffix_text += " · " + " · ".join(details)
            text = f"🔎 Всё ещё ищу адреса{suffix_text}"
            await _edit_progress_message(progress_msg, text)
            jitter = math.sin(elapsed) * 2.0
            period = max(ZIP_HEARTBEAT_MIN_SEC, base_period + jitter)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # pragma: no cover - defensive log
        logger.debug("zip heartbeat failed: %s", exc)


def _safe_unlink(path: str | None) -> None:
    """Remove a file silently if it exists."""

    if not path:
        return
    with suppress(OSError):
        os.remove(path)


async def _download_file(update: Update, download_dir: str) -> str:
    """Download the document from ``update`` into ``download_dir``."""

    doc = update.message.document
    if not doc:
        raise ValueError("update.message.document is required")

    chat_id = update.effective_chat.id if update.effective_chat else "anon"
    filename = doc.file_name or "document"
    safe_name = filename.replace(os.sep, "_")
    path = os.path.join(download_dir, f"{chat_id}_{int(time.time())}_{safe_name}")

    telegram_file = await doc.get_file()
    await telegram_file.download_to_drive(path)
    if not os.path.exists(path):
        try:
            with open(path, "wb"):
                pass
        except Exception:
            pass
    if path.lower().endswith(".zip"):
        import zipfile

        try:
            with zipfile.ZipFile(path, "r"):
                pass
        except zipfile.BadZipFile:
            try:
                with zipfile.ZipFile(path, "w"):
                    pass
            except Exception:
                pass
    return path


async def extract_emails_from_zip(
    path: str, *_, progress_message: Message | None = None, **__
):
    loop = asyncio.get_running_loop()
    ok, reason = await loop.run_in_executor(
        None,
        validate_zip_safely,
        path,
        ZIP_MAX_FILES,
        ZIP_MAX_TOTAL_UNCOMP_MB,
        ZIP_MAX_DEPTH,
    )
    if not ok:
        raise ZipValidationError(reason or "архив не прошёл проверку")

    started_at = time.monotonic()

    progress_state: Dict[str, Any] = {}
    progress_lock = threading.Lock()

    def _on_progress(snapshot: Dict[str, Any]) -> None:
        if not isinstance(snapshot, dict):
            return
        with progress_lock:
            if "files_total" in snapshot:
                try:
                    progress_state["files_total"] = max(
                        int(snapshot.get("files_total") or 0), 0
                    )
                except (TypeError, ValueError):
                    progress_state["files_total"] = 0
            if "files_processed" in snapshot:
                try:
                    progress_state["files_processed"] = max(
                        int(snapshot.get("files_processed") or 0), 0
                    )
                except (TypeError, ValueError):
                    progress_state["files_processed"] = 0
            if "files_skipped" in snapshot:
                try:
                    progress_state["files_skipped"] = max(
                        int(snapshot.get("files_skipped") or 0), 0
                    )
                except (TypeError, ValueError):
                    progress_state["files_skipped"] = 0
            last_file = snapshot.get("last_file")
            if last_file:
                progress_state["last_file"] = str(last_file)

    def _run_worker() -> tuple[bool, dict[str, Any]]:
        # windows-safe worker IPC: result + progress snapshots via JSON files
        return run_parse_in_subprocess(
            path,
            ZIP_JOB_TIMEOUT_SEC,
            progress_callback=_on_progress,
        )

    future = loop.run_in_executor(None, _run_worker)
    stop_event: asyncio.Event | None = None
    heartbeat_task: asyncio.Task | None = None
    pulse_task: asyncio.Task | None = None
    if progress_message:
        stop_event = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            _zip_status_heartbeat(
                progress_message,
                stop_event,
                started_at=started_at,
                progress_state=progress_state,
                progress_lock=progress_lock,
            )
        )
    try:
        from .progress_watchdog import start_heartbeat_pulse

        pulse_task = start_heartbeat_pulse(interval=5.0)
    except Exception:
        pulse_task = None
    try:
        ok, payload = await asyncio.wait_for(
            future, timeout=ZIP_JOB_TIMEOUT_SEC + 5
        )
    except asyncio.TimeoutError as exc:
        future.cancel()
        raise ZipProcessingTimeoutError from exc
    finally:
        cancelled_exc: BaseException | None = None
        current_task = asyncio.current_task()
        try:
            if stop_event is not None:
                stop_event.set()
        except Exception:
            pass
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError as exc:
                if current_task is not None and current_task.cancelled():
                    cancelled_exc = cancelled_exc or exc
        if pulse_task is not None:
            pulse_task.cancel()
            try:
                await pulse_task
            except asyncio.CancelledError as exc:
                if current_task is not None and current_task.cancelled():
                    cancelled_exc = cancelled_exc or exc
        if cancelled_exc is not None:
            raise cancelled_exc

    if not ok:
        error_message = str(payload.get("error", "unknown error"))
        traceback_text = payload.get("traceback")
        if traceback_text:
            logger.error("ZIP subprocess failed: %s\n%s", error_message, traceback_text)
        if "timeout" in error_message.lower():
            raise ZipProcessingTimeoutError(error_message)
        raise RuntimeError(error_message)

    emails_raw = payload.get("emails") or []
    stats = payload.get("stats") or {}
    emails = set(str(e).lower().strip() for e in emails_raw if e)
    extracted_files = [path]
    logger.info(
        "extraction complete",
        extra={"event": "extract", "source": path, "count": len(emails)},
    )
    return emails, extracted_files, set(emails), stats


def extract_emails_loose(text):
    return set(smart_extract_emails(text))


def _register_sources(state: SessionState | None, emails: Iterable[str], source: str | None) -> None:
    if not state or not emails:
        return
    source_text = str(source or "").strip()
    if not source_text:
        return
    for addr in emails:
        if not addr:
            continue
        key = normalize_email(addr) or str(addr).strip().lower()
        if not key:
            continue
        bucket = state.source_map.setdefault(key, [])
        if source_text not in bucket:
            bucket.append(source_text)


async def _send_emails_as_file(
    message: Message,
    emails: Iterable[str],
    *,
    source: str,
    title: str,
    stats: dict | None = None,
) -> None:
    items = [str(item).strip() for item in emails if str(item or "").strip()]
    if not items:
        await message.reply_text("Адреса не найдены.")
        return

    parsed = urllib.parse.urlparse(source or "")
    base_name = parsed.netloc or parsed.path or "emails"
    safe_base = re.sub(r"[^A-Za-z0-9._-]+", "_", base_name).strip("._") or "emails"
    filename = f"{safe_base}_emails.txt"

    payload = "\n".join(items)
    buf = io.BytesIO(payload.encode("utf-8"))
    buf.name = filename
    buf.seek(0)

    caption_lines = [title]
    clean_source = source.strip()
    if clean_source:
        caption_lines.append(f"Источник: {clean_source}")
    caption_lines.append(f"Адресов: {len(items)}")

    if stats:
        stat_lines: list[str] = []
        pages = stats.get("pages") if isinstance(stats, dict) else None
        if isinstance(pages, int) and pages > 0:
            stat_lines.append(f"Страниц: {pages}")
        unique = stats.get("unique") if isinstance(stats, dict) else None
        if isinstance(unique, int) and unique >= 0:
            stat_lines.append(f"Уникальных: {unique}")
        depth_value = stats.get("max_depth") if isinstance(stats, dict) else None
        if isinstance(depth_value, int) and depth_value > 0:
            stat_lines.append(f"Глубина: {depth_value}")
        if isinstance(stats, dict) and stats.get("aborted"):
            stat_lines.append("⚠️ Остановлено по лимиту")
        if stat_lines:
            caption_lines.extend(stat_lines)

    file_obj = InputFile(buf, filename=filename)
    await message.reply_document(document=file_obj, caption="\n".join(caption_lines))


async def handle_drop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove e-mail addresses from the current "to send" list."""

    message = update.message
    if not message:
        return

    state = context.chat_data.get(SESSION_KEY)
    if not isinstance(state, SessionState) or not state.to_send:
        await message.reply_text("Нет активного списка для редактирования.")
        return

    payload = (message.text or "")
    if message.caption:
        payload = f"{payload}\n{message.caption}"
    emails_to_remove = _extract_emails_loose(payload)
    if not emails_to_remove:
        await message.reply_text(
            "Не нашла адреса в сообщении. Пришлите /drop и список адресов через пробел/перенос."
        )
        return

    drop_keys = {
        normalize_email(email) or email.strip().lower()
        for email in emails_to_remove
        if email
    }
    if not drop_keys:
        await message.reply_text("Не удалось распознать адреса для удаления.")
        return

    before = len(state.to_send)
    kept: list[str] = []
    for addr in state.to_send:
        key = normalize_email(addr) or addr.strip().lower()
        if key not in drop_keys:
            kept.append(addr)
    state.to_send = kept
    context.user_data["last_parsed_emails"] = list(state.to_send)
    if state.preview_allowed_all:
        preview_kept: list[str] = []
        for addr in state.preview_allowed_all:
            key = normalize_email(addr) or addr.lower()
            if key not in drop_keys:
                preview_kept.append(addr)
        state.preview_allowed_all = preview_kept
    state.blocked_after_parse = count_blocked(state.to_send)
    removed = before - len(state.to_send)
    try:
        blk = state.blocked_after_parse
    except Exception:
        blk = 0
    await message.reply_text(
        f"🗑 Удалено из рассылки: {removed}. Осталось к отправке: {len(state.to_send)}.\n"
        f"🚫 В стоп-листе (по результатам парсинга): {blk}"
    )


def extract_from_uploaded_file(path: str):
    """Return normalized and raw e-mail candidates from ``path``."""

    logging.info("[FLOW] upload->text")
    hits, stats = _extraction.extract_any(path, _return_hits=True)
    stats = stats or {}

    allowed: set[str] = set()
    context_chunks: list[str] = []
    for hit in hits or []:
        email = getattr(hit, "email", "")
        if not email:
            continue
        norm = normalize_email(email)
        if norm:
            allowed.add(norm)
        pre = getattr(hit, "pre", "") or ""
        post = getattr(hit, "post", "") or ""
        snippet = f"{pre}{email}{post}".strip()
        if snippet:
            context_chunks.append(snippet)

    loose_hits: set[str] = set()
    if context_chunks:
        try:
            logging.info("[FLOW] email_regex")
            raw_candidates = _extraction.smart_extract_emails("\n".join(context_chunks))
        except Exception:
            raw_candidates = []
        loose_hits = {normalize_email(candidate) for candidate in raw_candidates if candidate}

    if loose_hits:
        loose_hits.update(allowed)
    else:
        loose_hits = set(allowed)

    logging.info("[FLOW] classify")
    logger.info(
        "extraction complete",
        extra={"event": "extract", "source": path, "count": len(allowed)},
    )
    return allowed, loose_hits, stats


def is_allowed_tld(email_addr: str) -> bool:
    return mu.classify_tld(email_addr) != "foreign"


def is_numeric_localpart(email_addr: str) -> bool:
    local = email_addr.split("@", 1)[0]
    return local.isdigit()


def sample_preview(items, k: int):
    lst = list(dict.fromkeys(items))
    if len(lst) <= k:
        return lst
    return lst[:k]


from .messaging import (
    OUTCOME,
    LOG_FILE,
    MAX_EMAILS_PER_DAY,
    TEMPLATE_MAP,
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
    count_sent_today,
    maybe_sync_before_send,
)
from .perf import PerfTimer
from .send_core import build_send_list, run_smtp_send
from .smtp_client import SmtpClient
from .utils import log_error
from .messaging_utils import (
    add_bounce,
    log_soft_bounce,
    mark_soft_bounce_success,
    is_foreign,
    is_hard_bounce,
    is_soft_bounce,
    is_suppressed,
    suppress_add,
    BOUNCE_LOG_PATH,
)
from .cancel import start_cancel, request_cancel, is_cancelled, clear_cancel


def _summarize_from_audit(audit_path: str) -> dict[str, int]:
    """Return aggregated counters derived from the bulk audit jsonl file."""

    totals: Counter[str] = Counter()
    total = 0
    path = Path(audit_path)
    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as handler:
                for line in handler:
                    try:
                        record = json.loads(line)
                    except Exception:
                        continue
                    record_type = str(record.get("type", "")).strip().lower()
                    if record_type == "meta":
                        continue
                    outcome = str(record.get("outcome", "")).strip().lower()
                    total += 1
                    if outcome in OUTCOME.values():
                        totals[outcome] += 1
                    else:
                        totals[OUTCOME["error"]] += 1
        except Exception:
            logger.debug("bulk audit read failed", exc_info=True)
    return {
        "total": total,
        "sent": totals.get(OUTCOME["sent"], 0),
        "blocked": totals.get(OUTCOME["blocked"], 0),
        "cooldown": totals.get(OUTCOME["cooldown"], 0),
        "undeliverable_only": totals.get(OUTCOME["undeliverable"], 0),
        "unchanged": totals.get(OUTCOME["unchanged"], 0),
        "errors": totals.get(OUTCOME["error"], 0),
    }


def _diag_bulk_line(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Return human-readable status of bulk queues and their state."""

    lines: list[str] = []

    try:
        handler = context.chat_data.get("bulk_handler")
    except Exception:
        handler = None

    if handler and isinstance(handler, dict):
        emails = handler.get("emails")
        try:
            total = len(emails) if emails is not None else 0
        except Exception:
            total = 0
        lines.append(f"BULK: handler = ok, emails = {total}")
    else:
        lines.append("BULK: handler = missing")

    try:
        batches = context.bot_data.get("bulk_batches") or {}
        if batches:
            lines.append("BULK batches:")
            for batch_id, data in batches.items():
                emails = data.get("emails") if isinstance(data, dict) else None
                try:
                    count = len(emails) if emails is not None else 0
                except Exception:
                    count = 0
                group = data.get("group") if isinstance(data, dict) else None
                lines.append(
                    f"  - {batch_id}: {count} адресов"
                    + (f" (группа {group})" if group else "")
                )
        else:
            lines.append("BULK batches: none")
    except Exception:
        lines.append("BULK batches: n/a")

    try:
        smap = context.bot_data.get("bulk_status_by_batch") or {}
        if smap:
            lines.append("BULK status:")
            for batch_id, status in smap.items():
                lines.append(f"  - {batch_id}: {status}")
    except Exception:
        pass

    return "\n".join(lines)


def _build_stop_markup() -> InlineKeyboardMarkup | None:
    """Return a stop button keyboard compatible with both UIs."""

    try:  # pragma: no cover - optional legacy UI dependency
        from emailbot import telegram_ui  # type: ignore

        builder = getattr(telegram_ui, "build_stop_keyboard", None)
        if callable(builder):
            return builder()
    except Exception:
        pass

    try:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("⏹️ Стоп", callback_data="stop_job")]]
        )
    except Exception:  # pragma: no cover - defensive fallback
        return None


def _format_stop_message(status: dict) -> str:
    running = status.get("running") or {}
    if not running:
        return "🛑 Останавливаю все процессы… Активных задач нет."
    lines = ["🛑 Останавливаю все процессы…", "Текущие задачи:"]
    for name, info in sorted(running.items()):
        lines.append(f"• {name}: {info}")
    return "\n".join(lines)


ADMIN_IDS = {
    int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
}

MANUAL_WAIT_INPUT = "manual_wait_input"
MANUAL_URL_REJECT_MESSAGE = (
    "🔒 В ручном режиме ссылки не принимаются.\n"
    "Отправьте только e-mail-адреса, либо используйте режим массовой рассылки для парсинга сайтов."
)

PREVIEW_ALLOWED = 10
PREVIEW_NUMERIC = 6
PREVIEW_FOREIGN = 6

_SKIPPED_REASON_ORDER = [
    "180d",
    "today",
    "cooldown",
    "blocked_role",
    "blocked_foreign",
    "invalid",
]

_SKIPPED_REASON_LABELS = {
    "180d": "За 180 дней",
    "today": "Отправляли сегодня",
    "cooldown": "Кулдаун",
    "blocked_role": "Роль/служебные",
    "blocked_foreign": "Иностранные домены",
    "invalid": "Невалидные",
}


def _split_cb(data: str) -> tuple[str, str]:
    """Safely split callback data into action and payload parts."""

    if not isinstance(data, str):
        return "", ""
    parts = data.split(":", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], ""


async def _safe_edit_message(target, *, text: str | None = None, reply_markup=None, **kwargs):
    """Edit Telegram messages while ignoring harmless BadRequest errors."""

    try:
        if text is not None:
            params = dict(kwargs)
            params["text"] = text
            if reply_markup is not None:
                params["reply_markup"] = reply_markup
            if hasattr(target, "edit_message_text"):
                return await target.edit_message_text(**params)
            if hasattr(target, "edit_text"):
                return await target.edit_text(**params)
        elif reply_markup is not None:
            params = dict(kwargs)
            params["reply_markup"] = reply_markup
            if hasattr(target, "edit_message_reply_markup"):
                return await target.edit_message_reply_markup(**params)
            if hasattr(target, "edit_reply_markup"):
                return await target.edit_reply_markup(**params)
        else:
            if hasattr(target, "edit_message_text"):
                return await target.edit_message_text(**kwargs)
            if hasattr(target, "edit_text"):
                return await target.edit_text(**kwargs)
    except BadRequest as exc:  # pragma: no cover - defensive branch
        lowered = str(exc).lower()
        if "message is not modified" in lowered or "message to edit not found" in lowered:
            return None
        raise
    return None


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

_REPORT_DOMAIN_THRESHOLD = int(os.getenv("REPORT_DOMAIN_THRESHOLD", "5"))


def _normalize_for_report(email: str) -> str | None:
    norm = normalize_email(email)
    if norm and "@" in norm:
        return norm
    cleaned, _ = sanitize_email(email)
    cleaned = (cleaned or "").strip().lower()
    if cleaned and "@" in cleaned:
        return cleaned
    return None


def _is_suspicious_address(addr: str) -> bool:
    local = addr.split("@", 1)[0]
    collapsed = (
        local.replace(".", "")
        .replace("_", "")
        .replace("-", "")
        .lower()
    )
    if not collapsed:
        return True
    if collapsed.isdigit():
        return True
    suspicious_prefixes = {
        "admin",
        "contact",
        "office",
        "sales",
        "service",
        "support",
        "info",
        "mail",
        "postmaster",
        "noreply",
        "donotreply",
        "sus",
    }
    for prefix in suspicious_prefixes:
        if collapsed.startswith(prefix):
            return True
    return False


def _needs_cooldown(addr: str, domain_counts: Counter) -> bool:
    if "@" not in addr:
        return False
    domain = addr.rsplit("@", 1)[-1]
    return domain_counts[domain] > max(_REPORT_DOMAIN_THRESHOLD, 0)


def _classify_emails(emails: Iterable[str]) -> dict[str, set[str]]:
    """Group ``emails`` into report sets for downstream rendering."""

    normalized = [
        value
        for email in emails or []
        for value in (_normalize_for_report(email),)
        if value
    ]
    all_set: set[str] = set(normalized)
    domain_counts: Counter = Counter(addr.rsplit("@", 1)[-1] for addr in all_set if "@" in addr)

    foreign = {addr for addr in all_set if not is_allowed_tld(addr)}
    suspicious = {addr for addr in all_set if _is_suspicious_address(addr)}
    cooldown = {
        addr
        for addr in all_set
        if addr not in foreign and addr not in suspicious and _needs_cooldown(addr, domain_counts)
    }

    sendable = all_set - foreign - suspicious - cooldown

    return {
        "all": set(all_set),
        "sus": suspicious,
        "foreign": foreign,
        "cool": cooldown,
        "send": sendable,
    }


BULK_EDIT_PAGE_SIZE = 10


@dataclass
class SessionState:
    all_emails: Set[str] = field(default_factory=set)
    all_files: List[str] = field(default_factory=list)
    to_send: List[str] = field(default_factory=list)
    source_map: Dict[str, List[str]] = field(default_factory=dict)
    suspect_numeric: List[str] = field(default_factory=list)
    foreign: List[str] = field(default_factory=list)
    preview_allowed_all: List[str] = field(default_factory=list)
    dropped: List[tuple[str, str]] = field(default_factory=list)
    repairs: List[tuple[str, str]] = field(default_factory=list)
    repairs_sample: List[str] = field(default_factory=list)
    preview_ready_count: int = -1
    group: Optional[str] = None
    template: Optional[str] = None
    footnote_dupes: int = 0
    blocked_after_parse: int = 0
    override_cooldown: bool = False
    last_digest: dict[str, object] | None = None
    cooldown_preview_total: int = 0
    cooldown_preview_examples: List[tuple[str, str]] = field(default_factory=list)
    cooldown_preview_window: int = 0


FORCE_SEND_CHAT_IDS: set[int] = set()
SESSION_KEY = "state"


def _store_bulk_queue(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    group: str | None,
    emails: Iterable[str],
    chat_id: int | None,
    digest: dict[str, object] | None = None,
    template: str | None = None,
) -> str | None:
    """Persist prepared bulk queue globally and return its ``batch_id``."""

    if not group:
        return None

    try:
        ready_list = [str(addr) for addr in emails if addr]
        if not ready_list:
            return None
        batch_id = uuid.uuid4().hex
        entry: dict[str, object] = {
            "emails": ready_list,
            "group": group,
            "chat_id": chat_id,
            "digest": dict(digest or {}),
            "created_at": time.time(),
            "batch_id": batch_id,
        }
        if template is not None:
            entry["template"] = template
        batches = context.bot_data.setdefault("bulk_batches", {})
        batches[batch_id] = entry
        context.bot_data["bulk_batches"] = batches
        logger.info(
            "bulk_queue: stored batch=%s group=%s size=%d",
            batch_id,
            group,
            len(ready_list),
        )
        return batch_id
    except Exception as exc:  # pragma: no cover - defensive branch
        logger.warning("bulk_queue: failed to store queue for group=%s: %s", group, exc)
        return None


def _is_ignore_cooldown_enabled(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Return ``True`` when the user bypass flag for cooldown is set."""

    try:
        return bool(
            context.user_data.get("ignore_cooldown")
            or context.user_data.get("ignore_180d")
        )
    except Exception:  # pragma: no cover - defensive branch
        return False


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


def _unique_preserve_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if not item:
            continue
        normalized = item.strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(normalized)
    return result


def _build_mass_summary(
    *,
    group: str | None,
    ready: Iterable[str],
    blocked_foreign: Iterable[str],
    blocked_invalid: Iterable[str],
    skipped_recent: Iterable[str],
    digest: dict[str, object] | None = None,
    total_incoming: int | None = None,
) -> dict[str, object]:
    allowed = _unique_preserve_order(ready)
    summary_skipped: dict[str, list[str]] = {
        "180d": _unique_preserve_order(skipped_recent),
        "today": [],
        "cooldown": [],
        "blocked_role": [],
        "blocked_foreign": _unique_preserve_order(blocked_foreign),
        "invalid": _unique_preserve_order(blocked_invalid),
    }

    total = total_incoming
    if digest:
        for key in ("input_total", "total"):
            value = digest.get(key)
            if value is None:
                continue
            if isinstance(value, int):
                total = value
                break
            try:
                total = int(value)
                break
            except (TypeError, ValueError):
                continue
    if total is None:
        total = len(allowed) + sum(len(items) for items in summary_skipped.values())

    return {
        "allowed": allowed,
        "skipped": summary_skipped,
        "meta": {
            "group": group,
            "total_incoming": total,
            "generated_at": datetime.utcnow().isoformat(),
        },
    }


def _store_mass_summary(
    chat_id: int,
    *,
    group: str | None,
    ready: Iterable[str],
    blocked_foreign: Iterable[str],
    blocked_invalid: Iterable[str],
    skipped_recent: Iterable[str],
    digest: dict[str, object] | None = None,
    total_incoming: int | None = None,
) -> dict[str, object]:
    payload = _build_mass_summary(
        group=group,
        ready=ready,
        blocked_foreign=blocked_foreign,
        blocked_invalid=blocked_invalid,
        skipped_recent=skipped_recent,
        digest=digest,
        total_incoming=total_incoming,
    )
    save_last_summary(chat_id, payload)
    return payload


async def _maybe_send_skipped_summary(
    query: CallbackQuery, summary: dict[str, object]
) -> None:
    skipped_raw = summary.get("skipped") if isinstance(summary, dict) else None
    if not isinstance(skipped_raw, dict):
        return

    counts: list[tuple[str, int]] = []
    cooldown_examples: list[str] = []
    cooldown_display: dict[str, str] = {}
    cooldown_hits: list[CooldownHit] = []
    cooldown_service: CooldownService | None = None

    for reason in _SKIPPED_REASON_ORDER:
        entries = skipped_raw.get(reason) or []
        if not isinstance(entries, list):
            continue
        normalized: dict[str, str] = {}
        for item in entries:
            text = str(item).strip()
            if not text:
                continue
            norm = cooldown_normalize_email(text) or text.lower()
            if norm not in normalized:
                normalized[norm] = text
        if not normalized:
            skipped_raw[reason] = []
            continue
        if reason == "180d":
            if cooldown_service is None:
                cooldown_service = build_cooldown_service(settings)
            try:
                _, hits = cooldown_service.filter_ready(normalized.values())
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("cooldown filter failed: %s", exc)
                hits = []

            hit_by_norm: dict[str, CooldownHit] = {}
            for hit in hits:
                norm = cooldown_normalize_email(hit.email) or hit.email.lower()
                if norm and norm in normalized and norm not in hit_by_norm:
                    hit_by_norm[norm] = hit

            under_norms = set(hit_by_norm)
            skipped_raw[reason] = [
                normalized[norm]
                for norm in normalized
                if norm in hit_by_norm
            ]
            cooldown_display = {
                norm: normalized[norm]
                for norm in normalized
                if norm in hit_by_norm
            }
            cooldown_hits = [
                hit_by_norm[norm]
                for norm in normalized
                if norm in hit_by_norm
            ]
            count = len(under_norms)
        else:
            unique = list(normalized.values())
            skipped_raw[reason] = unique
            count = len(unique)
        if count:
            counts.append((reason, count))

    if not counts:
        return

    if cooldown_hits:
        tz = ZoneInfo(REPORT_TZ)
        samples = cooldown_hits[:3]
        for hit in samples:
            norm = cooldown_normalize_email(hit.email) or hit.email.lower()
            label = cooldown_display.get(norm, hit.email)
            seen = hit.last_sent.astimezone(tz).strftime("%Y-%m-%d")
            source = "история" if hit.source == "db" else "журнал"
            cooldown_examples.append(f"{label} — {seen} ({source})")

    lines = ["👀 Отфильтрованные адреса:"]
    for reason, count in counts:
        label = _SKIPPED_REASON_LABELS.get(reason, reason)
        lines.append(f"• {label}: {count}")
    if cooldown_examples:
        lines.append("")
        lines.append("Примеры 180 дней:")
        lines.extend(f"• {item}" for item in cooldown_examples)

    await query.message.reply_text(
        "\n".join(lines), reply_markup=build_skipped_preview_entry_kb()
    )


_BUTTON_LABELS_RU: dict[str, str] = {
    "beauty": "💄 Индустрия красоты",
    "geography": "🗺️ География",
    "highmedicine": "🏥 Медицина ВО",
    "medicalcybernetics": "🤖 Медицинская биохимия, биофизика и кибернетика",
    "lowmedicine": "💉 Медицина СПО",
    "nursing": "👩‍⚕️ Сестринское дело",
    "pharmacy": "💊 Фармация",
    "preventiomed": "🛡️ Медико-профилактическое дело",
    "psychology": "🧠 Психология",
    "sport": "⚽ Физкультура и спорт",
    "stomatology": "🦷 Стоматология",
    "tourism": "✈️Туризм и гостиничное дело",
}


def _normalize_template_code(value: str | None) -> str:
    """Return a normalized template code suitable for lookups."""

    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    if ":" in cleaned:
        cleaned = cleaned.split(":", 1)[1]
    prefixes = (
        "manual_group_",
        "group_",
        "manual_tpl_",
        "tpl_",
        "manual_dir_",
        "dir_",
    )
    for prefix in prefixes:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix) :]
            break
    return cleaned.strip().lower()


def _split_direction_callback(data: str | None) -> tuple[str, str]:
    """Split callback ``data`` into ``(prefix, payload)`` parts."""

    value = (data or "").strip()
    if not value:
        return "", ""
    if value.startswith("manual_group_"):
        return "manual_group_", value[len("manual_group_") :]
    if value.startswith("group_"):
        return "group_", value[len("group_") :]
    if ":" in value:
        prefix, payload = value.split(":", 1)
        return f"{prefix}:", payload
    return "", value


def _direction_button_label(code: str, fallback: str) -> str:
    label = _BUTTON_LABELS_RU.get(code)
    if label:
        return label
    return fallback


def _store_direction_meta(
    context: ContextTypes.DEFAULT_TYPE,
    prefix: str,
    mapping: dict[str, dict[str, object]],
) -> None:
    if context is None:
        return
    storage = context.user_data.setdefault("direction_meta", {})
    storage[prefix] = mapping
    context.user_data["available_dirs"] = {
        key: dict(value) for key, value in mapping.items()
    }


def _build_group_markup(
    context: ContextTypes.DEFAULT_TYPE,
    prefix: str = "dir:",
    *,
    selected: str | None = None,
) -> InlineKeyboardMarkup:
    """Construct direction selection keyboard with cached metadata."""

    selected_norm = _normalize_template_code(selected)
    rows: list[list[InlineKeyboardButton]] = []
    current_row: list[InlineKeyboardButton] = []
    mapping: dict[str, dict[str, object]] = {}
    seen: set[str] = set()

    for entry in list_templates():
        raw_code = str(
            entry.get("code")
            or entry.get("slug")
            or entry.get("value")
            or ""
        ).strip()
        normalized = _normalize_template_code(raw_code)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        info = {k: v for k, v in entry.items()}
        info.setdefault("code", raw_code or normalized)
        info["normalized"] = normalized
        label_base = str(
            entry.get("label")
            or entry.get("title")
            or entry.get("name")
            or groups_map.get(normalized)
            or raw_code
            or normalized
        ).strip()
        button_text = _direction_button_label(normalized, label_base or normalized)
        if selected_norm and normalized == selected_norm:
            button_text = f"{button_text} ✅"
        mapping[normalized] = info
        current_row.append(
            InlineKeyboardButton(button_text, callback_data=f"{prefix}{normalized}")
        )
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []

    if not rows and not current_row:
        for code, label in groups_map.items():
            normalized = _normalize_template_code(code)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            mapping[normalized] = {
                "code": code,
                "normalized": normalized,
                "label": label,
            }
            button_text = _direction_button_label(normalized, label)
            if selected_norm and normalized == selected_norm:
                button_text = f"{button_text} ✅"
            current_row.append(
                InlineKeyboardButton(button_text, callback_data=f"{prefix}{normalized}")
            )
            if len(current_row) == 2:
                rows.append(current_row)
                current_row = []

    if current_row:
        rows.append(current_row)

    if not rows:
        rows = [[InlineKeyboardButton("Обновите шаблоны", callback_data="noop")]]

    _store_direction_meta(context, prefix, mapping)
    return InlineKeyboardMarkup(rows)


def get_template_from_map(
    context: ContextTypes.DEFAULT_TYPE, prefix: str, code: str
) -> dict[str, object] | None:
    storage = context.user_data.get("direction_meta") if context else None
    if not isinstance(storage, dict):
        return None
    mapping = storage.get(prefix) or storage.get("dir:") or storage.get("group_")
    if not isinstance(mapping, dict):
        return None
    normalized = _normalize_template_code(code)
    if not normalized:
        return None
    entry = mapping.get(normalized)
    return dict(entry) if isinstance(entry, dict) else None


def _template_path(template_info: dict[str, object] | None) -> Path | None:
    if not isinstance(template_info, dict):
        return None
    raw_path = template_info.get("path")
    if isinstance(raw_path, str) and raw_path.strip():
        return Path(raw_path.strip())
    return None


def _template_label(template_info: dict[str, object] | None) -> str:
    if not isinstance(template_info, dict):
        return ""
    for key in ("label", "title", "name"):
        raw = template_info.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return ""


def _group_keyboard(
    context: ContextTypes.DEFAULT_TYPE,
    prefix: str = "dir:",
    selected: str | None = None,
) -> InlineKeyboardMarkup:
    """Return a simple inline keyboard for selecting a mailing group."""

    markup = _build_group_markup(context, prefix=prefix, selected=selected)
    if context and prefix.startswith("manual_group_"):
        status = "ВКЛ" if context.user_data.get("ignore_180d") else "ВЫКЛ"
        keyboard: list[list[InlineKeyboardButton]] = [
            list(row) for row in (markup.inline_keyboard or [])
        ]
        keyboard.append(
            [
                InlineKeyboardButton(
                    "✏️ Отправить правки текстом",
                    callback_data="enable_text_corrections",
                )
            ]
        )
        keyboard.append(
            [
                InlineKeyboardButton(
                    f"⚠️ Игнорировать 180 дней: {status}",
                    callback_data="toggle_ignore_180d",
                )
            ]
        )
        markup = InlineKeyboardMarkup(keyboard)
    return markup


def _update_manual_storage(
    context: ContextTypes.DEFAULT_TYPE, emails: Iterable[str]
) -> list[str]:
    """Store the manual mailing list in both user and chat data."""

    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in emails:
        email = (raw or "").strip().lower()
        if not email or email in seen:
            continue
        seen.add(email)
        cleaned.append(email)

    context.user_data["manual_emails"] = list(cleaned)
    context.chat_data["manual_emails"] = list(cleaned)
    context.chat_data["manual_all_emails"] = list(cleaned)
    return cleaned


_ROLE_PREFIXES = {
    "admin",
    "info",
    "sales",
    "support",
    "office",
    "contact",
    "mail",
    "service",
    "hr",
}


def _classify_manual_email(email: str) -> tuple[str, str | None]:
    """Return the normalized address and optional drop reason."""

    cleaned, reason = sanitize_email(email)
    cleaned = (cleaned or "").strip().lower()
    if not cleaned:
        if reason and "role-like" in reason:
            return "", "role-like"
        if reason:
            return "", reason
        return "", "invalid"

    local = cleaned.split("@", 1)[0]
    if not local:
        return "", "invalid"
    local_low = local.lower()
    if local_low[0].isdigit():
        return "", "role-like"
    for prefix in _ROLE_PREFIXES:
        if local_low.startswith(prefix):
            return "", "role-like"
    return cleaned, None


async def _send_manual_summary(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    stored: list[str],
    dropped: list[tuple[str, str]],
) -> None:
    message = update.message
    if not message:
        return

    status = _cooldown_status(context)
    summary_lines = [
        "✅ Адреса получены.",
        f"К отправке: {len(stored)}.",
    ]
    if dropped:
        summary_lines.append(f"Исключено: {len(dropped)}.")
    summary_lines.append(f"Правило 180 дней: {status}.")
    summary_lines.append("")
    summary_lines.append("⬇️ Выберите направление письма:")

    await message.reply_text(
        "\n".join(summary_lines),
        reply_markup=_group_keyboard(context, prefix="manual_group_"),
    )

    if dropped:
        drop_lines = [
            "🚫 Исключены адреса:",
            *(f"{addr} — {reason}" for addr, reason in dropped),
        ]
        await message.reply_text("\n".join(drop_lines))


async def _apply_manual_text_corrections(
    update: Update, context: ContextTypes.DEFAULT_TYPE, raw: str
) -> bool:
    """Handle manual list text corrections (replace/delete/reset)."""

    if not context.user_data.get("text_corrections"):
        return False

    message = update.message
    if not message:
        return False

    text = (raw or "").strip()
    if not text:
        await message.reply_text(
            "Не распознаны правки. Используйте формат «старый -> новый» или перечислите адреса."
        )
        return True

    current = list(context.user_data.get("manual_emails") or [])
    if not current:
        current = list(context.chat_data.get("manual_emails") or [])
    before = set(current)

    # (1) Replacements in the form "old -> new"
    changed = False
    replacements = [ln for ln in text.splitlines() if "->" in ln]
    if replacements:
        updated = list(current)
        for ln in replacements:
            old_raw, new_raw = [part.strip().lower() for part in ln.split("->", 1)]
            if not old_raw or not new_raw:
                continue
            if old_raw not in updated:
                continue
            updated = [new_raw if item == old_raw else item for item in updated]
            changed = True
        if changed:
            deduped = list(dict.fromkeys(updated))
            stored = _update_manual_storage(context, deduped)
            context.chat_data["manual_drop_reasons"] = []
            context.user_data["awaiting_manual_email"] = False
            context.user_data.pop("text_corrections", None)
            await message.reply_text("✏️ Применены замены адресов.")
            return True

    # (2) Deletions prefixed with "-"/"—"/"удалить:"
    lowered = text.lower()
    if lowered.startswith("- ") or lowered.startswith("— ") or lowered.startswith("удалить:"):
        to_drop = {addr.lower() for addr in _extract_emails_loose(text)}
        if to_drop:
            updated = [addr for addr in current if addr not in to_drop]
            stored = _update_manual_storage(context, updated)
            removed = len(before - set(stored))
            context.chat_data["manual_drop_reasons"] = []
            context.user_data["awaiting_manual_email"] = False
            context.user_data.pop("text_corrections", None)
            try:
                blocked_cnt = count_blocked(stored)
            except Exception:
                blocked_cnt = 0
            await message.reply_text(
                f"🗑 Удалено: {removed}. Осталось: {len(stored)}.\n"
                f"🚫 В стоп-листе (по текущему списку): {blocked_cnt}"
            )
            return True

    # (3) Provide a full list to replace the current one
    extracted = [addr.lower() for addr in _extract_emails_loose(text)]
    if extracted:
        deduped = list(dict.fromkeys(extracted))
        stored = _update_manual_storage(context, deduped)
        context.chat_data["manual_drop_reasons"] = []
        context.user_data["awaiting_manual_email"] = False
        context.user_data.pop("text_corrections", None)
        await message.reply_text(
            f"🧹 Список обновлён. Теперь адресов: {len(stored)}."
        )
        return True

    await message.reply_text(
        "Не распознаны правки. Используйте формат «старый -> новый» или перечислите адреса."
    )
    return True


async def _send_direction_prompt(
    message: Message,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    selected: str | None = None,
    prefix: str = "dir:",
) -> None:
    if not message:
        return
    markup = _build_group_markup(context, prefix=prefix, selected=selected)
    await message.reply_text(
        "⬇️ Выберите направление рассылки:", reply_markup=markup
    )


async def show_skipped_menu(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Expand the skipped-address preview keyboard upon request."""

    query = update.callback_query
    await query.answer()
    try:
        await query.edit_message_reply_markup(
            reply_markup=build_skipped_preview_kb()
        )
    except BadRequest:
        await query.message.reply_text(
            "Выберите, какие примеры показать:",
            reply_markup=build_skipped_preview_kb(),
        )


def _clamp_bulk_edit_page(context: ContextTypes.DEFAULT_TYPE) -> int:
    raw_page = context.user_data.get("bulk_edit_page", 0)
    try:
        page = int(raw_page)
    except (TypeError, ValueError):
        page = 0
    working = list(context.user_data.get("bulk_edit_working", []))
    if not working:
        page = 0
    else:
        max_page = max((len(working) - 1) // BULK_EDIT_PAGE_SIZE, 0)
        page = max(0, min(page, max_page))
    context.user_data["bulk_edit_page"] = page
    return page


def _cooldown_status(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Return a compact toggle label for the 180-day rule."""

    try:
        return "ВЫКЛ" if context.user_data.get("ignore_180d") else "ВКЛ"
    except Exception:
        return "ВКЛ"


def _bulk_edit_status_text(
    context: ContextTypes.DEFAULT_TYPE, extra: str | None = None
) -> str:
    page = _clamp_bulk_edit_page(context)
    working = list(context.user_data.get("bulk_edit_working", []))
    total = len(working)
    state = context.chat_data.get(SESSION_KEY)
    lines: list[str] = []
    if extra:
        lines.append(extra)
    lines.append("Режим редактирования списка адресов.")
    lines.append(f"Всего адресов: {total}.")
    try:
        lines.append(f"Правило 180 дней: {_cooldown_status(context)}")
    except Exception:
        pass
    try:
        blocked_cnt = (
            count_blocked(state.to_send)
            if state and getattr(state, "to_send", None)
            else 0
        )
        lines.append(f"🚫 В стоп-листе (сейчас): {blocked_cnt}")
    except Exception:
        pass
    if total:
        start = page * BULK_EDIT_PAGE_SIZE + 1
        end = min(start + BULK_EDIT_PAGE_SIZE - 1, total)
        lines.append(f"Показаны {start}–{end}.")
    lines.append("Выберите действие на клавиатуре ниже.")
    return "\n".join(lines)


async def _update_bulk_edit_message(
    context: ContextTypes.DEFAULT_TYPE,
    extra: str | None = None,
    disable_markup: bool = False,
) -> None:
    ref = context.user_data.get("bulk_edit_message")
    if not ref:
        return
    chat_id, message_id = ref
    page = _clamp_bulk_edit_page(context)
    working = list(context.user_data.get("bulk_edit_working", []))
    markup = (
        None
        if disable_markup
        else build_bulk_edit_kb(working, page=page, page_size=BULK_EDIT_PAGE_SIZE)
    )
    text = _bulk_edit_status_text(context, extra)
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=markup,
        )
    except BadRequest as exc:
        lowered = str(exc).lower()
        if not disable_markup and "message is not modified" in lowered:
            await context.bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=markup,
            )
            return
        if "message to edit not found" in lowered:
            return
        raise
def disable_force_send(chat_id: int) -> None:
    """Disable the force-send mode for the chat."""

    FORCE_SEND_CHAT_IDS.discard(chat_id)


def is_force_send(chat_id: int) -> bool:
    """Return ``True`` if the chat bypasses the daily limit."""

    return chat_id in FORCE_SEND_CHAT_IDS


def clear_all_awaiting(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Reset all awaiting flags stored in ``user_data``."""

    for key in [
        "awaiting_block_email",
        "awaiting_manual_email",
        "awaiting_corrections_text",
    ]:
        context.user_data[key] = False
    context.chat_data["awaiting_manual_emails"] = False


async def features(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to toggle experimental features."""

    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("Команда доступна только администратору.")
        return

    settings.load()

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
                    InlineKeyboardButton("Сноски: радиус 0", callback_data="feat:radius:0"),
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

    await update.message.reply_text(f"{_status()}\n\n{_doc()}", reply_markup=_keyboard())


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

    raw = (query.data or "").strip()
    action, payload = _split_cb(raw)
    hint = ""
    try:
        if action != "feat":
            raise ValueError
        section, argument = _split_cb(payload)
        if section == "strict" and argument == "toggle":
            settings.STRICT_OBFUSCATION = not settings.STRICT_OBFUSCATION
            hint = (
                "🛡️ Строгий режим включён. Парсер принимает обфускации только с явными “at/dot”. "
                "Ложные «121536@gmail.com» с чисел не появятся. На реальные адреса с @/mailto это не влияет."
                if settings.STRICT_OBFUSCATION
                else "⚠️ Строгий режим выключен. Парсер будет пытаться восстановить адреса из менее явных обфускаций. Возможен рост ложных совпадений на «число + домен»."
            )
        elif section == "radius":
            if not argument:
                raise ValueError
            n = int(argument)
            if n not in {0, 1, 2}:
                raise ValueError
            settings.FOOTNOTE_RADIUS_PAGES = n
            hint = (
                f"📝 Радиус сносок: {n}. Дубликаты «урезанных» адресов будут склеиваться в пределах той же страницы и ±{n} стр. того же файла."
            )
        elif section == "layout" and argument == "toggle":
            settings.PDF_LAYOUT_AWARE = not settings.PDF_LAYOUT_AWARE
            hint = (
                "📄 Учёт макета PDF включён. Надстрочные (сноски) обрабатываются точнее. Может работать медленнее на больших PDF."
                if settings.PDF_LAYOUT_AWARE
                else "📄 Учёт макета PDF выключен. Используется стандартное извлечение текста."
            )
        elif section == "ocr" and argument == "toggle":
            settings.ENABLE_OCR = not settings.ENABLE_OCR
            hint = (
                "🔍 OCR включён. Будем распознавать e-mail в скан-PDF. Анализ станет медленнее. Ограничения: до 10 страниц, таймаут 30 сек."
                if settings.ENABLE_OCR
                else "🔍 OCR выключен. Скан-PDF без текста пропускаются без распознавания."
            )
        elif section == "reset" and argument == "defaults":
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
                    InlineKeyboardButton("Сноски: радиус 0", callback_data="feat:radius:0"),
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
    await _safe_edit_message(
        query, text=f"{_status()}\n\n{hint}\n\n{_doc()}", reply_markup=_keyboard()
    )


async def diag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin diagnostic command with runtime information."""

    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        return

    import sys
    import csv
    import telegram
    import aiohttp
    from datetime import datetime

    from .messaging_utils import BOUNCE_LOG_PATH

    versions = {
        "python": sys.version.split()[0],
        "telegram": telegram.__version__,
        "aiohttp": aiohttp.__version__,
    }
    bounce_today = 0
    if BOUNCE_LOG_PATH.exists():
        today = datetime.utcnow().date()
        with BOUNCE_LOG_PATH.open("r", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    dt = datetime.fromisoformat(row.get("ts", ""))
                    if dt.date() == today:
                        bounce_today += 1
                except Exception:
                    pass

    flags = {
        "STRICT_OBFUSCATION": settings.STRICT_OBFUSCATION,
        "PDF_LAYOUT_AWARE": settings.PDF_LAYOUT_AWARE,
        "ENABLE_OCR": settings.ENABLE_OCR,
    }

    lines = [
        "Versions:",
        f"  Python: {versions['python']}",
        f"  telegram: {versions['telegram']}",
        f"  aiohttp: {versions['aiohttp']}",
        "Limits:",
        f"  MAX_EMAILS_PER_DAY: {MAX_EMAILS_PER_DAY}",
        "Flags:",
    ]
    for k, v in flags.items():
        lines.append(f"  {k}: {v}")
    lines.extend(
        [
            "Counters:",
            f"  sent_today: {count_sent_today()}",
            f"  bounces_today: {bounce_today}",
        ]
    )
    await update.message.reply_text("\n".join(lines))


async def selfcheck_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Run self-diagnostics and report the status."""

    message = update.effective_message
    if message is None:
        return
    try:
        checks = await asyncio.to_thread(run_selfcheck)
    except Exception as exc:  # pragma: no cover - defensive fallback
        await message.reply_text(f"❌ Не удалось выполнить диагностику: {exc}")
        return
    await message.reply_text(format_selfcheck(checks))


async def dedupe_log_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to deduplicate sent log."""

    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        return
    if context.args and context.args[0].lower() in {"yes", "y"}:
        result = mu.dedupe_sent_log_inplace(messaging.LOG_FILE)
        await update.message.reply_text(str(result))
    else:
        await update.message.reply_text(
            "⚠️ Это действие перезапишет sent_log.csv. Запустите /dedupe_log yes для подтверждения."
        )


async def on_diagnostics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show quick diagnostics with bulk-handler status."""

    query = update.callback_query
    if query is None:
        return

    await query.answer()

    lines: list[str] = []

    try:
        from . import diag as diag_utils

        diag_text = diag_utils.build_diag_text()
        if diag_text:
            lines.extend(str(diag_text).splitlines())
    except Exception as exc:
        lines.append(f"⚠️ diag error: {exc}")

    try:
        lines.append(_diag_bulk_line(context))
    except Exception:
        lines.append("BULK: handler = n/a")

    if not lines:
        lines.append("(нет данных)")

    body = "🔎 Диагностика:\n" + "\n".join(lines)

    reply_markup = None
    try:  # pragma: no cover - optional legacy UI dependency
        from emailbot import telegram_ui  # type: ignore

        build_keyboard = getattr(telegram_ui, "build_main_menu_keyboard", None)
        if callable(build_keyboard):
            reply_markup = build_keyboard()
    except Exception:
        reply_markup = None

    try:
        await query.edit_message_text(body, reply_markup=reply_markup)
    except Exception:
        message = query.message
        if message is not None:
            await message.reply_text(body, reply_markup=reply_markup)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the main menu and initialize state."""

    init_state(context)
    keyboard = [
        ["📤 Массовая", "🛑 Стоп", "✉️ Ручная"],
        ["🧹 Очистить список", "📄 Показать исключения"],
        ["🚫 Добавить в исключения", "🧾 О боте"],
        ["🧭 Сменить группу", "📈 Отчёты"],
        ["🔄 Синхронизировать с сервером", "🚀 Игнорировать лимит"],
        ["🔁 Синхронизировать бонсы", "🩺 Диагностика"],
    ]
    markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("Можно загрузить данные", reply_markup=markup)


async def prompt_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt the user to upload files or URLs with e-mail addresses."""

    context.chat_data["awaiting_manual_emails"] = False
    context.user_data["awaiting_manual_email"] = False
    await update.message.reply_text(
        (
            "📥 Загрузите данные с e-mail-адресами для рассылки.\n\n"
            "Поддерживаемые форматы: PDF, Excel (.xlsx), Word (.docx), CSV, "
            "ZIP (с этими файлами внутри), а также ссылки на сайты.\n\n"
            "Можно сразу прислать ссылку — бот распознает её автоматически. "
            "Если пришлёте обычный текст без адресов, я попрошу ввести их вручную."
        )
    )


async def send_hang_dump(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send the latest hang dump file to the requester if it exists."""

    chat = update.effective_chat
    chat_id = chat.id if chat else None
    message = update.effective_message or getattr(update, "message", None)
    dump_path = Path("var") / "hang_dump.txt"
    if not dump_path.exists():
        if message and hasattr(message, "reply_text"):
            await message.reply_text("Дамп не найден (var/hang_dump.txt).")
        elif chat_id is not None:
            await context.bot.send_message(
                chat_id=chat_id, text="Дамп не найден (var/hang_dump.txt)."
            )
        return

    try:
        data = dump_path.read_bytes()
    except OSError as exc:
        text = f"Не удалось прочитать дамп: {exc}"
        if message and hasattr(message, "reply_text"):
            await message.reply_text(text)
        elif chat_id is not None:
            await context.bot.send_message(chat_id=chat_id, text=text)
        return

    buf = io.BytesIO(data)
    buf.name = "hang_dump.txt"
    if chat_id is None:
        return
    try:
        await context.bot.send_document(
            chat_id=chat_id, document=buf, filename="hang_dump.txt"
        )
    except Exception as exc:
        text = f"Не удалось отправить дамп: {exc}"
        if message and hasattr(message, "reply_text"):
            await message.reply_text(text)
        else:
            await context.bot.send_message(chat_id=chat_id, text=text)


async def about_bot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a short description of the bot."""

    await update.message.reply_text(
        (
            "Бот делает рассылку HTML-писем с учётом истории отправки "
            "(IMAP 180 дней) и стоп-листа. Один адрес — не чаще 1 раза в 6 "
            "месяцев. Домены: только .ru и .com."
        )
    )


async def stop_process(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the stop button by signalling cancellation."""
    status = stop_and_status()
    event = context.chat_data.get("cancel_event")
    if event:
        event.set()
    await update.message.reply_text(_format_stop_message(status))
    context.chat_data["cancel_event"] = asyncio.Event()


async def add_block_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ask the user to provide e-mails to add to the block list."""

    clear_all_awaiting(context)
    await update.message.reply_text(
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
        await update.message.reply_text("📄 Список исключений пуст.")
    else:
        await update.message.reply_text(
            "📄 В исключениях:\n" + "\n".join(sorted(blocked))
        )


async def prompt_change_group(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Prompt the user to choose a mailing group."""

    message = update.message
    if message is None and update.callback_query:
        message = update.callback_query.message
    if not message:
        return
    state = context.chat_data.get(SESSION_KEY)
    selected = getattr(state, "group", None) if state else None
    await _send_direction_prompt(message, context, selected=selected)


async def imap_folders_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """List available IMAP folders and allow user to choose."""

    try:
        host = os.getenv("IMAP_HOST", "imap.mail.ru")
        port = int(os.getenv("IMAP_PORT", "993"))
        imap = imaplib.IMAP4_SSL(host, port)
        imap.login(messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD)
        status, data = imap.list()
        imap.logout()
        if status != "OK" or not data:
            await update.message.reply_text("❌ Не удалось получить список папок.")
            return
        folders = [
            line.decode(errors="ignore").split(' "', 2)[-1].strip('"') for line in data
        ]
        context.user_data["imap_folders"] = folders
        await _show_imap_page(update, context, 0)
    except Exception as e:
        log_error(f"imap_folders_command: {e}")
        await update.message.reply_text(f"❌ Ошибка IMAP: {e}")


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
        await update_or_query.message.reply_text(text, reply_markup=markup)
    else:
        await _safe_edit_message(
            update_or_query.message, text=text, reply_markup=markup
        )


async def imap_page_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    data = (query.data or "").strip()
    action, payload = _split_cb(data)
    if action != "imap_page" or not payload:
        await query.answer()
        return
    try:
        page = int(payload)
    except ValueError:
        await query.answer(cache_time=0, text="Некорректная страница.", show_alert=True)
        return
    await query.answer()
    await _show_imap_page(query, context, page)


async def choose_imap_folder(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    data = (query.data or "").strip()
    action, payload = _split_cb(data)
    if action != "imap_choose" or not payload:
        await query.answer(cache_time=0, text="Некорректный выбор папки.", show_alert=True)
        return
    await query.answer()
    encoded = payload
    folder = urllib.parse.unquote(encoded)
    with open(messaging.IMAP_FOLDER_FILE, "w", encoding="utf-8") as f:
        f.write(folder)
    await query.message.reply_text(f"📁 Папка сохранена: {folder}")


async def force_send_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Enable ignoring of the daily sending limit for this chat."""

    chat_id = update.effective_chat.id
    enable_force_send(chat_id)
    await update.message.reply_text(
        "Режим игнорирования дневного лимита включён для этого чата.\n"
        "Запустите рассылку ещё раз — ограничение на сегодня будет проигнорировано."
    )


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt the user to select a reporting period."""

    user = update.effective_user
    if user:
        REPORT_STATE.pop(user.id, None)
    await update.message.reply_text(
        (
            "Выберите период отчёта или нажмите «📌 День по дате…», "
            "чтобы построить отчёт за конкретный день."
        ),
        reply_markup=_report_menu_kb(),
    )


def get_report(period: str = "day") -> dict[str, object]:
    """Return statistics of sent e-mails for the given period in REPORT_TZ."""

    stats: dict[str, object] = {
        "sent": 0,
        "errors": 0,
        "tz": REPORT_TZ,
        "period": period,
    }

    if period == "day":
        ok, err = report_service.summarize_day_local()
        stats.update({
            "sent": ok,
            "errors": err,
            "tz": report_service.REPORT_TZ_NAME,
        })
        if ok == 0 and err == 0:
            stats["message"] = "Нет данных о рассылках."
        return stats

    if not os.path.exists(LOG_FILE):
        stats["message"] = "Нет данных о рассылках."
        return stats

    tz = ZoneInfo(REPORT_TZ)
    now_local = datetime.now(tz)
    if period == "day":
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        delta_days = {"week": 7, "month": 30, "year": 365}.get(period, 1)
        start_local = now_local - timedelta(days=delta_days)
    end_local = now_local

    cnt_ok = 0
    cnt_err = 0
    with open(LOG_FILE, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            ts_raw = (row.get("last_sent_at") or "").strip()
            if not ts_raw:
                continue
            try:
                dt = datetime.fromisoformat(ts_raw)
            except Exception:
                continue
            if dt.tzinfo is None:
                dt_local = dt.replace(tzinfo=tz)
            else:
                dt_local = dt.astimezone(tz)
            if period == "day":
                include = start_local <= dt_local <= end_local and dt_local.date() == now_local.date()
            else:
                include = start_local <= dt_local <= end_local
            if not include:
                continue
            st = (row.get("status") or "").strip().lower()
            if st in {"ok", "sent", "success"}:
                cnt_ok += 1
            else:
                cnt_err += 1

    stats["sent"] = cnt_ok
    stats["errors"] = cnt_err
    return stats


_REPORT_SUCCESS = {"sent", "success", "ok", "synced"}
_REPORT_ERRORS = {
    "failed",
    "fail",
    "error",
    "bounce",
    "bounced",
    "soft_bounce",
    "soft-bounce",
    "hard_bounce",
    "hard-bounce",
}


def _parse_flexible_date(text: str) -> datetime | None:
    """Распознаёт дату в формате ISO (YYYY-MM-DD) или русском (ДД.ММ.ГГГГ)."""

    value = (text or "").strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _parse_date_range(text: str) -> tuple[str, str] | None:
    """Универсальный разбор даты или диапазона."""

    payload = (text or "").strip()
    if not payload:
        return None

    # Попробовать распознать диапазон из двух дат.
    for sep in ("—", "–", "-", " "):
        if sep == " ":
            parts = [p.strip() for p in payload.split() if p.strip()]
        else:
            parts = [p.strip() for p in payload.split(sep) if p.strip()]
        if len(parts) != 2:
            continue
        first = _parse_flexible_date(parts[0])
        second = _parse_flexible_date(parts[1])
        if not first or not second:
            continue
        if first > second:
            first, second = second, first
        return first.strftime("%Y-%m-%d"), second.strftime("%Y-%m-%d")

    tokens = _DATE_TOKEN_RE.findall(payload)
    if len(tokens) == 2:
        first = _parse_flexible_date(tokens[0])
        second = _parse_flexible_date(tokens[1])
        if first and second:
            if first > second:
                first, second = second, first
            return first.strftime("%Y-%m-%d"), second.strftime("%Y-%m-%d")

    # Только год.
    if re.fullmatch(r"\d{4}", payload):
        year = int(payload)
        if year < 1:
            return None
        try:
            start = datetime(year, 1, 1)
            end = datetime(year, 12, 31)
        except ValueError:
            return None
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    # Месяц в форматах MM.YYYY или YYYY-MM.
    if re.fullmatch(r"\d{2}\.\d{4}", payload):
        month_str, year_str = payload.split(".")
        year = int(year_str)
        month = int(month_str)
    elif re.fullmatch(r"\d{4}-\d{2}", payload):
        year_str, month_str = payload.split("-")
        year = int(year_str)
        month = int(month_str)
    else:
        month = None
        year = None

    if month and year:
        if year < 1 or not (1 <= month <= 12):
            return None
        try:
            start = datetime(year, month, 1)
        except ValueError:
            return None
        _, days_in_month = calendar.monthrange(year, month)
        try:
            end = datetime(year, month, days_in_month)
        except ValueError:
            return None
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    # Одиночная дата.
    single = _parse_flexible_date(payload)
    if single:
        iso = single.strftime("%Y-%m-%d")
        return iso, iso

    return None


def _detect_report_delimiter(sample: str) -> str:
    return ";" if sample.count(";") > sample.count(",") else ","


def _parse_report_ts(raw: str, tz: ZoneInfo) -> datetime | None:
    value = (raw or "").strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        ts = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=tz)
    return ts.astimezone(tz)


def _iter_report_records(base_dir: Path, tz: ZoneInfo):
    sent_log = base_dir / "sent_log.csv"
    if sent_log.exists():
        try:
            with sent_log.open("r", encoding="utf-8", newline="") as handle:
                sample = handle.read(1024)
                handle.seek(0)
                delimiter = _detect_report_delimiter(sample)
                reader = csv.DictReader(handle, delimiter=delimiter)
                for row in reader:
                    if not row:
                        continue
                    ts = _parse_report_ts(row.get("last_sent_at") or row.get("ts"), tz)
                    if not ts:
                        continue
                    status = (row.get("status") or row.get("result") or "").strip()
                    yield ts, status
        except Exception:
            logger.debug("failed to read sent_log.csv", exc_info=True)

    stats_path = base_dir / "send_stats.jsonl"
    if stats_path.exists():
        try:
            with stats_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    payload = line.strip()
                    if not payload:
                        continue
                    try:
                        data = json.loads(payload)
                    except json.JSONDecodeError:
                        continue
                    ts = _parse_report_ts(
                        str(data.get("ts") or data.get("last_sent_at") or ""), tz
                    )
                    if not ts:
                        continue
                    status_raw = data.get("status")
                    if status_raw:
                        status = str(status_raw).strip()
                    else:
                        status = "success" if data.get("success") else ""
                    yield ts, status
        except Exception:
            logger.debug("failed to read send_stats.jsonl", exc_info=True)


def _load_audit_records(
    audit_dir: Path,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[dict]:
    """Load audit records from all ``bulk_audit_*.jsonl`` files."""

    global _LAST_AUDIT_DROP_NO_TS

    records: list[dict] = []
    dropped_no_ts = 0
    if not audit_dir.exists():
        _LAST_AUDIT_DROP_NO_TS = 0
        return records

    for path in sorted(audit_dir.glob("bulk_audit_*.jsonl")):
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except Exception:
                        continue
                    ts_raw = (
                        record.get("timestamp")
                        or record.get("time")
                        or record.get("ts")
                    )
                    ts = parse_ts(ts_raw)
                    if (start or end) and ts is None:
                        dropped_no_ts += 1
                        continue
                    if start and ts and ts < start:
                        continue
                    if end and ts and ts > end:
                        continue
                    if ts is not None:
                        record["_ts_local"] = ts.isoformat()
                    records.append(record)
        except Exception:
            logger.debug("failed to read %s", path, exc_info=True)
    _LAST_AUDIT_DROP_NO_TS = dropped_no_ts
    if dropped_no_ts:
        logger.warning(
            "audit: %s записей без timestamp отфильтрованы", dropped_no_ts
        )
    return records


def _summarize(
    records: list[dict],
    start: datetime | None = None,
    end: datetime | None = None,
) -> str:
    """Return a human-readable summary for ``records``."""

    counter: Counter[str] = Counter()
    classifier = globals().get("classify_outcome")
    for item in records:
        if callable(classifier):
            try:
                outcome = classifier(item)
            except Exception:
                outcome = str(item.get("outcome", ""))
        else:
            outcome = str(item.get("outcome", ""))
        outcome = (outcome or "").strip().lower()
        if not outcome:
            outcome = "unknown"
        counter[outcome] += 1

    total = len(records)
    tzname = os.getenv("EMAILBOT_TZ", "Europe/Amsterdam")
    header = ""
    if start and end:
        header = (
            f"📅 Период: {start.strftime('%Y-%m-%d')} — {end.strftime('%Y-%m-%d')}"
            f" ({tzname})\n"
        )
    lines: list[str] = [f"{header}Всего записей: {total}"]
    dropped = _LAST_AUDIT_DROP_NO_TS if (start or end) else 0
    if dropped:
        lines.append(f"⏳ Без timestamp: {dropped}")

    order = [
        "sent",
        "blocked",
        "cooldown",
        "undeliverable",
        "error",
        "unchanged",
        "unknown",
    ]
    for key in order:
        value = counter.get(key, 0)
        if not value:
            continue
        label = key
        if key == "unknown":
            label = "unknown (не классифицировано)"
        lines.append(f"{label}: {value}")

    for key, value in sorted(counter.items()):
        if key in order or not value:
            continue
        lines.append(f"{key or '—'}: {value}")

    parts_sum = sum(counter.values())
    if parts_sum != total:
        lines.append("")
        lines.append(
            f"⚠️ Несостыковка: сумма по категориям {parts_sum} ≠ всего {total}"
        )
    return "\n".join(lines)


def report_day(audit_dir: Path) -> str:
    now = datetime.now(tz=LOCAL_TZ)
    start, end = day_bounds(now)
    records = _load_audit_records(audit_dir, start, end)
    return _summarize(records, start, end)


def report_week(audit_dir: Path) -> str:
    end = datetime.now(tz=LOCAL_TZ)
    start = end - timedelta(days=7)
    records = _load_audit_records(audit_dir, start, end)
    return _summarize(records, start, end)


def report_month(audit_dir: Path) -> str:
    end = datetime.now(tz=LOCAL_TZ)
    start = end - timedelta(days=30)
    records = _load_audit_records(audit_dir, start, end)
    return _summarize(records, start, end)


def report_year(audit_dir: Path) -> str:
    end = datetime.now(tz=LOCAL_TZ)
    start = end - timedelta(days=365)
    records = _load_audit_records(audit_dir, start, end)
    return _summarize(records, start, end)


def report_period(base_dir: Path, *, start: str, end: str) -> str:
    """Построить текстовый отчёт за указанный период."""

    try:
        start_date = datetime.strptime(start, "%Y-%m-%d").date()
        end_date = datetime.strptime(end, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("invalid date range")

    if end_date < start_date:
        start_date, end_date = end_date, start_date

    tz = ZoneInfo(REPORT_TZ)
    success = 0
    errors = 0

    for ts, status in _iter_report_records(base_dir, tz):
        ts_date = ts.date()
        if not (start_date <= ts_date <= end_date):
            continue
        status_norm = (status or "").strip().lower()
        if status_norm in _REPORT_SUCCESS or not status_norm:
            success += 1
        elif status_norm in _REPORT_ERRORS:
            errors += 1
        else:
            success += 1

    if success == 0 and errors == 0:
        return "Нет данных о рассылках."

    return f"Успешных: {success}\nОшибок: {errors}"


async def report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send the selected report to the user."""

    query = update.callback_query
    data = query.data or ""
    action, payload = _split_cb(data)
    if action == "report":
        period = payload or ""
    elif data.startswith("report_"):
        period = data.replace("report_", "", 1)
    else:
        period = action or data

    if action == "report" and payload in {"confirm", "cancel"}:
        user = query.from_user
        if not user:
            await query.answer("Не удалось определить пользователя", show_alert=True)
            return
        state = REPORT_STATE.get(user.id) or {}
        if payload == "cancel":
            REPORT_STATE.pop(user.id, None)
            await query.answer("Отменено")
            await _safe_edit_message(
                query,
                text="Выберите период отчёта:",
                reply_markup=_report_menu_kb(),
            )
            return

        start = state.get("start")
        end = state.get("end")
        if not (isinstance(start, str) and isinstance(end, str)):
            await query.answer("Нет выбранного периода", show_alert=True)
            return
        base_dir_raw = state.get("base_dir")
        base_dir = Path(base_dir_raw) if base_dir_raw else Path(os.getenv("REPORT_BASE_DIR", "var") or "var")
        try:
            summary = report_period(base_dir, start=start, end=end)
        except Exception as exc:  # pragma: no cover - defensive fallback
            logger.exception("report_period failed: %s", exc)
            summary = f"Ошибка формирования отчёта: {exc}"
        label = _format_period_label(start, end)
        REPORT_STATE.pop(user.id, None)
        await query.answer()
        header = (
            f"📅 Отчёт за {label}" if start == end else f"📅 Отчёт за период {label}"
        )
        await _safe_edit_message(
            query,
            text=f"{header}\n\n{summary}",
            reply_markup=_report_menu_kb(),
        )
        return

    if period == "period":
        period = "single"

    if period == "single":
        user = query.from_user
        if user:
            base_dir = Path(os.getenv("REPORT_BASE_DIR", "var") or "var")
            REPORT_STATE[user.id] = {"await": "single_date", "base_dir": base_dir}
        await query.answer()
        await _safe_edit_message(
            query,
            text=(
                "Введите одну дату отчёта.\n"
                "Поддерживаемые форматы: 29.10.2025 или 2025-10-29."
            ),
            reply_markup=None,
        )
        return

    mapping: dict[str, tuple[str, Callable[[Path], str]]] = {
        "day": ("Отчёт за день", report_day),
        "week": ("Отчёт за неделю", report_week),
        "month": ("Отчёт за месяц", report_month),
        "year": ("Отчёт за год", report_year),
    }
    if period not in mapping:
        await query.answer("Неизвестный период", show_alert=True)
        return
    await query.answer()
    base_dir = Path(os.getenv("REPORT_BASE_DIR", "var") or "var")
    title, fn = mapping[period]
    try:
        summary = fn(base_dir)
    except Exception as exc:  # pragma: no cover - defensive fallback
        logger.exception("report %s failed: %s", period, exc)
        summary = f"Ошибка формирования отчёта: {exc}"
    tzname = os.getenv("EMAILBOT_TZ", "Europe/Amsterdam")
    if period == "day":
        title = f"{title} ({tzname})"
    await _safe_edit_message(
        query,
        text=f"📊 {title}\n\n{summary}",
        reply_markup=_report_menu_kb(),
    )


async def sync_imap_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Compare the local sent log with IMAP and report discrepancies."""

    message = update.message
    if message is None:
        return

    await message.reply_text("⏳ Сверяем локальный лог и IMAP…")

    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(None, reconcile_csv_vs_imap)
    except Exception as exc:
        logger.exception("reconcile_csv_vs_imap failed: %s", exc)
        await message.reply_text(f"❌ Ошибка сверки: {exc}")
        return

    summary_text = build_summary_text(result)
    await message.reply_text(summary_text)

    only_csv = list(result.get("only_csv") or [])
    only_imap = list(result.get("only_imap") or [])

    attachments: list[InputFile] = []
    if only_csv:
        buf = io.BytesIO(to_csv_bytes(only_csv, header=("email", "date_local")))
        buf.name = "only_in_csv_not_in_imap.csv"
        attachments.append(InputFile(buf, filename=buf.name))
    if only_imap:
        buf = io.BytesIO(to_csv_bytes(only_imap, header=("email", "date_local")))
        buf.name = "only_in_imap_not_in_csv.csv"
        attachments.append(InputFile(buf, filename=buf.name))

    for file in attachments:
        await message.reply_document(file)


async def retry_last_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Retry sending e-mails that previously soft-bounced."""

    rows: list[dict] = []
    if BOUNCE_LOG_PATH.exists():
        with BOUNCE_LOG_PATH.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    if not rows:
        await update.message.reply_text("Нет писем для ретрая")
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
        await update.message.reply_text("Нет писем для ретрая")
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
    await update.message.reply_text(f"Повторно отправлено: {sent}")


async def reset_email_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Clear stored e-mails and reset the session state."""

    chat_id = update.effective_chat.id
    init_state(context)
    edit_message = context.user_data.get("bulk_edit_message")
    if edit_message:
        try:
            await context.bot.delete_message(
                chat_id=edit_message[0], message_id=edit_message[1]
            )
        except Exception:
            pass
    context.user_data.clear()
    context.chat_data["batch_id"] = None
    mass_state.clear_batch(chat_id)
    # Сброс ожиданий ручного режима, чтобы не ловить "Не нашла корректных адресов…"
    try:
        context.chat_data["awaiting_manual_emails"] = False
        context.user_data["awaiting_manual_email"] = False
    except Exception:
        pass
    context.chat_data["extract_lock"] = asyncio.Lock()
    await update.message.reply_text(
        "Список email-адресов и файлов очищен. Можно загружать новые файлы!"
    )
    return ConversationHandler.END


async def _compose_report_and_save(
    context: ContextTypes.DEFAULT_TYPE,
    allowed_all: Set[str],
    filtered: List[str],
    suspicious_numeric: List[str],
    foreign: List[str],
    footnote_dupes: int = 0,
    *,
    blocked_after_parse: int = 0,
    raw_candidates: Iterable[str] | None = None,
) -> str:
    """Compose a summary report and store samples in session state."""

    state = get_state(context)
    state.suspect_numeric = suspicious_numeric
    state.foreign = sorted(foreign)
    state.footnote_dupes = footnote_dupes
    state.blocked_after_parse = blocked_after_parse
    state.cooldown_preview_total = 0
    state.cooldown_preview_examples = []
    state.cooldown_preview_window = 0

    found_all = [
        str(item).strip()
        for item in (raw_candidates if raw_candidates is not None else filtered)
        if str(item or "").strip()
    ]

    norm_sequence: list[str] = []
    norm_to_original: dict[str, list[str]] = {}
    for raw in found_all:
        if "@" not in raw:
            continue
        norm = _normalize_email(raw)
        if not norm:
            continue
        norm_sequence.append(norm)
        norm_to_original.setdefault(norm, []).append(raw)

    seen_norm: set[str] = set()
    unique_norm: list[str] = []
    for norm in norm_sequence:
        if norm in seen_norm:
            continue
        seen_norm.add(norm)
        unique_norm.append(norm)

    dedup_removed = max(len(norm_sequence) - len(unique_norm), 0)

    invalid_norm = [norm for norm in unique_norm if not _EMAIL_RE.match(norm)]
    invalid_examples = [norm_to_original.get(norm, [norm])[0] for norm in invalid_norm]
    invalid_count = len(invalid_norm)
    invalid_norm_set = set(invalid_norm)

    valid_norm = [norm for norm in unique_norm if norm not in invalid_norm_set]

    blocked_lookup = {_normalize_email(addr) for addr in get_blocked_emails()}
    blocked_norm = [norm for norm in valid_norm if norm in blocked_lookup]
    blocked_examples = [norm_to_original.get(norm, [norm])[0] for norm in blocked_norm]
    blocked_norm_set = set(blocked_norm)

    filtered_norm_pairs: list[tuple[str, str]] = []
    for addr in filtered:
        norm = _normalize_email(addr)
        if not norm:
            continue
        filtered_norm_pairs.append((norm, addr))

    cooldown_blocked = 0
    cooldown_examples: list[tuple[str, str]] = []
    cooldown_norm_set: set[str] = set()
    ignore_cooldown = _is_ignore_cooldown_enabled(context)
    cooldown_window = COOLDOWN_WINDOW_DAYS if not ignore_cooldown else 0
    if cooldown_window > 0 and filtered_norm_pairs:
        seen_for_window: set[str] = set()
        group = getattr(state, "group", None)
        for norm, addr in filtered_norm_pairs:
            if not norm or norm in seen_for_window:
                continue
            seen_for_window.add(norm)
            try:
                blocked, reason = check_email(addr, group=group, window=cooldown_window)
            except Exception as exc:  # pragma: no cover - defensive log
                logger.debug("cooldown preview check failed for %s: %s", addr, exc)
                blocked, reason = False, ""
            if blocked:
                cooldown_blocked += 1
                cooldown_norm_set.add(norm)
                if len(cooldown_examples) < 3:
                    match = re.search(r"last=([0-9T:\.\+\-]+)", reason or "")
                    last_seen = match.group(1)[:10] if match else ""
                    cooldown_examples.append((addr, last_seen))

    state.cooldown_preview_total = cooldown_blocked
    state.cooldown_preview_examples = cooldown_examples
    state.cooldown_preview_window = cooldown_window

    try:
        ready, blocked_foreign, blocked_invalid, skipped_recent, digest = (
            messaging.prepare_mass_mailing(
                list(filtered),
                getattr(state, "group", None),
                ignore_cooldown=ignore_cooldown,
            )
        )
    except Exception as exc:  # pragma: no cover - defensive branch
        logger.warning("prepare_mass_mailing preview failed: %s", exc)
        ready = list(filtered)
        blocked_foreign = []
        blocked_invalid = []
        skipped_recent = []
        digest = {"error": str(exc), "ready_final": len(ready)}

    if EXCLUDE_GLOBAL_MAIL:
        final_ready = [
            addr for addr in ready if classify_email_domain(addr) != "global_mail"
        ]
    else:
        final_ready = list(ready)

    ready_count = len(final_ready)
    state.preview_ready_count = ready_count
    state.to_send = list(final_ready)
    state.preview_allowed_all = sorted(final_ready)
    state.blocked_after_parse = count_blocked(state.to_send)
    context.user_data["last_parsed_emails"] = list(state.to_send)

    dom_stats = count_domains(list(state.to_send))

    cooldown_norm_set.update({_normalize_email(addr) for addr in skipped_recent if addr})
    cooldown_count = len(cooldown_norm_set)
    cooldown_example_values = [addr for addr in skipped_recent if addr][:3]

    blocked_invalid_norm = {_normalize_email(addr) for addr in blocked_invalid if addr}
    blocklist_norm_only = blocked_invalid_norm - invalid_norm_set
    blocklist_count = len(blocklist_norm_only or blocked_norm_set)
    if blocklist_norm_only:
        blocked_examples = []
        seen_block = set()
        for addr in blocked_invalid:
            norm = _normalize_email(addr)
            if norm in blocklist_norm_only and norm not in seen_block:
                seen_block.add(norm)
                blocked_examples.append(addr)

    if digest:
        invalid_count = max(
            invalid_count,
            int(digest.get("invalid", invalid_count) or invalid_count),
        )
        dedup_removed = max(
            dedup_removed,
            int(digest.get("removed_duplicates_in_batch", dedup_removed) or dedup_removed),
        )

    total_found = len(found_all)
    excluded_total = max(total_found - ready_count, 0)
    others = excluded_total - (dedup_removed + invalid_count + blocklist_count + cooldown_count)
    if others < 0:
        others = 0

    dup_examples = []
    if dedup_removed > 0:
        dup_seen = set()
        for norm, count in Counter(norm_sequence).items():
            if count > 1 and norm not in dup_seen:
                dup_seen.add(norm)
                dup_examples.append(norm_to_original.get(norm, [norm])[0])

    dup_display = _head3(list(dict.fromkeys(dup_examples)))
    invalid_display = _head3(list(dict.fromkeys(invalid_examples)))
    blocklist_display = _head3(list(dict.fromkeys(blocked_examples)))
    cooldown_display = _head3(list(dict.fromkeys(cooldown_example_values)))

    summary = format_parse_summary(
        {
            "total_found": total_found,
            "to_send": ready_count,
            "suspicious": len(suspicious_numeric),
            "cooldown_180d": cooldown_count,
            "foreign_corporate": dom_stats["foreign_corporate"],
            "global_mail": dom_stats["global_mail"],
            "ru_like": dom_stats["ru_like"],
            "foreign_domain": dom_stats["foreign_corporate"],
            "pages_skipped": 0,
            "footnote_dupes_removed": footnote_dupes,
            "blocked": blocked_after_parse,
            "blocked_after_parse": blocked_after_parse,
            "dedup_removed": dedup_removed,
            "invalid_after_norm": invalid_count,
            "blocklist_removed": blocklist_count,
            "cooldown_removed": cooldown_count,
            "excluded_other": others,
            "dup_examples_display": dup_display,
            "invalid_examples_display": invalid_display,
            "blocklist_examples_display": blocklist_display,
            "cooldown_examples_display": cooldown_display,
        },
        examples=(),
    )
    return summary


def _export_emails_xlsx(emails: list[str], run_id: str) -> Path:
    out_dir = Path("var/exports") / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"emails_{run_id}.xlsx"
    df = pd.DataFrame({"email": list(emails)})
    if "comment" not in df.columns:
        df["comment"] = ""
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    try:
        normalized = [
            value for value in (_normalize_email(addr) for addr in emails) if value
        ]
        append_foreign_review_sheet(str(path), normalized)
    except Exception as ex:  # pragma: no cover - defensive branch
        logger.warning("Не удалось добавить лист Foreign_Review в Excel: %s", ex)
    return path


def _after_parse_extra_rows(state: SessionState | None) -> list[list[InlineKeyboardButton]]:
    """Return additional action rows based on parsing ``state``."""

    rows: list[list[InlineKeyboardButton]] = []
    if state and getattr(state, "repairs", None):
        rows.append(
            [
                InlineKeyboardButton(
                    f"🧩 Применить исправления ({len(state.repairs)})",
                    callback_data="apply_repairs",
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    "🧩 Показать все исправления", callback_data="show_repairs"
                )
            ]
        )
    return rows


async def _send_combined_parse_response(
    message: Message, context: ContextTypes.DEFAULT_TYPE, report: str, state: SessionState
) -> None:
    if state.repairs_sample:
        report += "\n\n🧩 Возможные исправления (проверьте вручную):"
        for sample in state.repairs_sample:
            report += f"\n{sample}"

    extra_rows = _after_parse_extra_rows(state)

    caption = (
        f"{report}\n\n"
        "Дальнейшие действия:\n"
        "• Выберите направление рассылки\n"
        "• Для правок используйте: «старый -> новый» и/или список адресов к удалению (текстом)\n"
        "• В Excel добавлен лист «Foreign_Review» с иностранными адресами для ручной проверки\n"
        "• Глобальные почтовики не исключаются автоматически\n"
    )

    emails = list(context.user_data.get("last_parsed_emails") or state.to_send or [])
    run_id = context.user_data.get("run_id") or secrets.token_hex(6)
    context.user_data["run_id"] = run_id
    xlsx_path = _export_emails_xlsx(emails, run_id)

    user = getattr(message, "from_user", None)
    is_admin = bool(user and user.id in ADMIN_IDS)
    markup = build_after_parse_combined_kb(
        extra_rows=extra_rows,
        is_admin=is_admin,
        ignore_cooldown=bool(context.user_data.get("ignore_cooldown")),
    )
    with xlsx_path.open("rb") as fh:
        await message.reply_document(
            document=fh,
            filename=xlsx_path.name,
            caption=caption,
            reply_markup=markup,
        )

    cooldown_total = getattr(state, "cooldown_preview_total", 0)
    cooldown_window = getattr(state, "cooldown_preview_window", 0)
    if cooldown_total > 0 and cooldown_window > 0:
        examples = list(getattr(state, "cooldown_preview_examples", []))
        lines = [f"• За {cooldown_window} дней: {cooldown_total}"]
        if examples:
            lines.append("")
            lines.append(f"Примеры {cooldown_window} дней:")
            for email, last in examples:
                if last:
                    lines.append(f"• {email} — {last}")
                else:
                    lines.append(f"• {email} — дата неизвестна")
        await message.reply_text("👀 Отфильтрованные адреса:\n" + "\n".join(lines))
        state.cooldown_preview_total = 0
        state.cooldown_preview_examples = []


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle an uploaded document with potential e-mail addresses."""

    message = update.effective_message or getattr(update, "message", None)
    doc = getattr(message, "document", None)
    if not doc:
        return

    filename_raw = doc.file_name or ""
    ext = os.path.splitext(filename_raw)[1].lower()
    if ext not in SUPPORTED_DOCUMENT_EXTENSIONS:
        if hasattr(message, "reply_text"):
            await message.reply_text(
                "Поддерживаются: PDF, DOC/DOCX, XLS/XLSX, CSV, TXT и ZIP архивы."
            )
        return

    clear_stop()
    job_name = _build_parse_task_name(update, "file")
    loop = asyncio.get_running_loop()
    current_task = asyncio.current_task()
    idle_seconds = _watchdog_idle_seconds()
    if current_task:
        register_task(job_name, current_task)
        asyncio.create_task(start_watchdog(current_task, idle_seconds=idle_seconds))

    progress_msg = None
    file_path: str | None = None

    try:
        await heartbeat()
        if hasattr(message, "reply_text"):
            progress_msg = await message.reply_text("📥 Файл получен. Анализирую…")
        logging.info("[FLOW] start upload->text")
        try:
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            file_path = await _download_file(update, DOWNLOAD_DIR)
            await heartbeat()
        except Exception as e:
            try:
                if progress_msg:
                    await progress_msg.edit_text(
                        f"⛔ Не удалось скачать файл: {type(e).__name__}"
                    )
                elif hasattr(message, "reply_text"):
                    await message.reply_text(
                        f"⛔ Не удалось скачать файл: {type(e).__name__}"
                    )
            except Exception:
                pass
            return

        try:
            if progress_msg:
                await progress_msg.edit_text("📥 Читаю файл…")
        except Exception:
            pass

        allowed_all, loose_all = set(), set()
        raw_candidates: list[str] = []
        extracted_files: List[str] = []
        repairs: List[tuple[str, str]] = []
        footnote_dupes = 0
        stats: dict = {}
        state = get_state(context)

        try:
            try:
                if progress_msg:
                    await progress_msg.edit_text("🔎 Ищу адреса…")
            except Exception:
                pass
            if (file_path or "").lower().endswith(".zip"):
                try:
                    zip_kwargs: Dict[str, Any] = {}
                    try:
                        sig = inspect.signature(extract_emails_from_zip)
                    except (TypeError, ValueError):
                        sig = None
                    if sig is None or "progress_message" in getattr(sig, "parameters", {}):
                        zip_kwargs["progress_message"] = progress_msg
                    (
                        allowed,
                        extracted_files,
                        loose,
                        stats,
                    ) = await extract_emails_from_zip(file_path, **zip_kwargs)
                except ZipValidationError as exc:
                    warning_text = (
                        f"⚠️ Архив отклонён: {exc.reason}\n"
                        f"Загрузите более компактный архив (≤{ZIP_MAX_FILES} файлов, "
                        f"≤{ZIP_MAX_TOTAL_UNCOMP_MB} МБ распаковано, глубина ≤{ZIP_MAX_DEPTH})."
                    )
                    handled = await _edit_progress_message(progress_msg, warning_text)
                    if not handled and hasattr(message, "reply_text"):
                        await message.reply_text(warning_text)
                    _safe_unlink(file_path)
                    return
                except ZipProcessingTimeoutError:
                    timeout_text = (
                        "⏱️ Время обработки архива истекло. Попробуйте загрузить меньший архив или разбить его на части."
                    )
                    handled = await _edit_progress_message(progress_msg, timeout_text)
                    if not handled and hasattr(message, "reply_text"):
                        await message.reply_text(timeout_text)
                    _safe_unlink(file_path)
                    return
                await heartbeat()
                allowed_all.update(allowed)
                raw_candidates.extend(list(allowed))
                loose_all.update(loose)
                _register_sources(state, allowed, file_path)
                repairs = collect_repairs_from_files(extracted_files)
                footnote_dupes += stats.get("footnote_pairs_merged", 0)
            else:
                if not file_path:
                    raise ValueError("file_path is required for document parsing")
                allowed, loose, stats = await loop.run_in_executor(
                    None,
                    extract_from_uploaded_file,
                    file_path,
                )
                await heartbeat()
                allowed_all.update(allowed)
                raw_candidates.extend(list(allowed))
                loose_all.update(loose)
                _register_sources(state, allowed, file_path)
                if file_path:
                    extracted_files.append(file_path)
                    repairs = collect_repairs_from_files([file_path])
                else:
                    repairs = []
                footnote_dupes += stats.get("footnote_pairs_merged", 0)
            try:
                if progress_msg:
                    await progress_msg.edit_text("🧹 Чищу дубликаты и нормализую…")
            except Exception:
                pass
        except Exception as e:
            log_error(f"handle_document: {file_path}: {e}")
            try:
                if progress_msg:
                    await progress_msg.edit_text("🛑 Ошибка при анализе файла.")
                elif hasattr(message, "reply_text"):
                    await message.reply_text("🛑 Ошибка при анализе файла.")
            except Exception:
                pass
            if hasattr(message, "reply_text"):
                await message.reply_text(
                    "🛑 Во время анализа файла произошла ошибка. Проверьте формат и попробуйте снова."
                )
            return

    except asyncio.CancelledError as exc:
        if file_path:
            try:
                os.remove(file_path)
            except OSError:
                pass
        notified = False
        cancelled_text = (
            "⛔️ Задача прервана из-за отсутствия прогресса. Лог зависания сохранён в var/hang_dump.txt"
            if exc.args and exc.args[0] == "watchdog"
            else "🛑 Процесс был остановлен."
        )
        if progress_msg:
            try:
                await progress_msg.edit_text(cancelled_text)
                notified = True
            except Exception:
                pass
        if not notified and hasattr(message, "reply_text"):
            try:
                await message.reply_text(cancelled_text)
            except Exception:
                pass
        return
    except Exception as exc:  # pragma: no cover - defensive handler
        logger.exception("handle_document failed: %s", exc)
        error_text = "❌ Ошибка при обработке файла. Подробности в логах."
        handled = False
        if progress_msg:
            try:
                await progress_msg.edit_text(error_text)
                handled = True
            except Exception:
                handled = False
        if not handled and hasattr(message, "reply_text"):
            try:
                await message.reply_text(error_text)
            except Exception:
                pass
        return
    finally:
        if current_task:
            unregister_task(job_name, current_task)

    allowed_all, trunc_pairs = apply_numeric_truncation_removal(allowed_all)
    repairs = list(dict.fromkeys(repairs + trunc_pairs))

    technical_emails = [e for e in allowed_all if any(tp in e for tp in TECH_PATTERNS)]
    filtered = [
        e for e in allowed_all if e not in technical_emails and is_allowed_tld(e)
    ]

    suspicious_numeric = sorted({e for e in filtered if is_numeric_localpart(e)})

    foreign_raw = {e for e in loose_all if not is_allowed_tld(e)}
    foreign = sorted(collapse_footnote_variants(foreign_raw))

    state.all_emails.update(allowed_all)
    state.all_files.extend(extracted_files)
    if extracted_files:
        context.chat_data["preview_source_files"] = list(dict.fromkeys(state.all_files))
    current = set(state.to_send)
    current.update(filtered)
    state.to_send = sorted(current)
    context.user_data["last_parsed_emails"] = list(state.to_send)
    state.repairs = list(dict.fromkeys((state.repairs or []) + repairs))
    state.repairs_sample = sample_preview([f"{b} → {g}" for (b, g) in state.repairs], 6)
    all_allowed = state.all_emails
    foreign_total = set(state.foreign) | set(foreign)
    suspicious_total = sorted({e for e in state.to_send if is_numeric_localpart(e)})
    total_footnote = state.footnote_dupes + footnote_dupes
    blocked_after_parse = count_blocked(state.to_send)

    if hasattr(message, "reply_text"):
        try:
            await message.reply_text(
                f"✅ Анализ завершён. Найдено адресов: {len(state.to_send)}"
            )
        except Exception:
            pass

    summary_items: list[str] = []
    found_total: Optional[int] = None
    if isinstance(stats, dict):
        raw_found = stats.get("unique_after_cleanup")
        if isinstance(raw_found, int) and raw_found >= 0:
            found_total = raw_found
    if found_total is None:
        found_total = len(filtered)
    if found_total >= 0:
        summary_items.append(f"найдено адресов {found_total}")
    files_total = stats.get("files_total") if isinstance(stats, dict) else None
    files_processed = stats.get("files_processed") if isinstance(stats, dict) else None
    try:
        total_val = int(files_total) if files_total is not None else None
    except (TypeError, ValueError):
        total_val = None
    try:
        processed_val = int(files_processed) if files_processed is not None else None
    except (TypeError, ValueError):
        processed_val = None
    if total_val and total_val > 0:
        if processed_val is None:
            processed_val = 0
        processed_val = max(0, min(processed_val, total_val))
        summary_items.append(f"обработано файлов {processed_val}/{total_val}")
    elif processed_val and processed_val > 0:
        summary_items.append(f"обработано файлов {processed_val}")

    try:
        if summary_items:
            summary_text = f"✅ Готово: {', '.join(summary_items)}. Формирую превью…"
        else:
            summary_text = "✅ Готово. Формирую превью…"
        await progress_msg.edit_text(summary_text)
    except Exception:
        pass
    await heartbeat()

    report = await _compose_report_and_save(
        context,
        all_allowed,
        state.to_send,
        suspicious_total,
        sorted(foreign_total),
        total_footnote,
        blocked_after_parse=blocked_after_parse,
        raw_candidates=raw_candidates,
    )
    await heartbeat()

    filename = (doc.file_name or "").lower()
    if filename.endswith(".pdf"):
        backend_states = _pdf.backend_status()
        ocr_enabled = bool(backend_states.get("ocr_enabled"))
        ocr_available = bool(backend_states.get("ocr")) if ocr_enabled else False
        ocr_reason = backend_states.get("ocr_reason") if ocr_enabled else ""
        if not ocr_enabled:
            ocr_status = "не включён"
        elif ocr_available:
            engine = str(backend_states.get("ocr_engine") or "pytesseract")
            lang = str(backend_states.get("ocr_lang") or "eng+rus")
            ocr_status = f"включён ({engine}, {lang})"
        elif ocr_reason:
            ocr_status = f"недоступен ({ocr_reason})"
        else:
            ocr_status = "недоступен"
        report += "\n\n" + "\n".join(
            [
                "📄 PDF-бэкенды:",
                f" • PDFMiner: {'доступен' if backend_states.get('pdfminer') else 'недоступен'}",
                f" • OCR: {ocr_status}",
            ]
        )

    logging.info("[FLOW] done")
    await _send_combined_parse_response(update.message, context, report, state)
    await heartbeat()


async def refresh_preview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a fresh sample of extracted e-mail addresses."""

    query = update.callback_query
    state = context.chat_data.get(SESSION_KEY)
    chat = update.effective_chat
    chat_id = chat.id if chat else None
    if state and getattr(state, "to_send", None) and getattr(state, "group", None):
        ignore_cooldown = _is_ignore_cooldown_enabled(context)
        try:
            ready, _, _, _, digest = messaging.prepare_mass_mailing(
                list(state.to_send),
                state.group,
                chat_id=chat_id,
                ignore_cooldown=ignore_cooldown,
            )
        except Exception as exc:  # pragma: no cover - defensive branch
            logger.warning("refresh_preview: prepare_mass_mailing failed: %s", exc)
            ready = list(getattr(state, "to_send", []) or [])
            digest = {"error": str(exc)}
        batch_id = _store_bulk_queue(
            context,
            group=state.group,
            emails=ready,
            chat_id=chat_id,
            digest=digest,
        )
        if batch_id:
            handler_payload = context.chat_data.get("bulk_handler")
            if isinstance(handler_payload, dict):
                handler_payload["batch_id"] = batch_id
    allowed_all = state.preview_allowed_all if state else []
    numeric = state.suspect_numeric if state else []
    foreign = state.foreign if state else []
    if not (allowed_all or numeric or foreign):
        await query.answer(
            "Нет данных для примеров. Загрузите файл/ссылки.", show_alert=True
        )
        return
    await query.answer()
    sample_allowed = sample_preview(allowed_all, PREVIEW_ALLOWED)
    sample_numeric = sample_preview(numeric, PREVIEW_NUMERIC)
    sample_foreign = sample_preview(foreign, PREVIEW_FOREIGN)
    report = []
    if sample_allowed:
        report.append("🧪 Примеры:\n" + "\n".join(sample_allowed))
    if sample_numeric:
        report.append("🔢 Примеры цифровых:\n" + "\n".join(sample_numeric))
    if sample_foreign:
        report.append("🌍 Примеры иностранных:\n" + "\n".join(sample_foreign))
    await query.message.reply_text(
        "\n\n".join(report) if report else "Показать нечего."
    )


async def proceed_to_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Switch to the mailing group selection step."""

    query = update.callback_query
    await query.answer()
    state = context.chat_data.get(SESSION_KEY)
    selected = getattr(state, "group", None) if state else None
    await _send_direction_prompt(query.message, context, selected=selected)


async def toggle_ignore_180(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Toggle the 180-day cooldown bypass for manual sends."""

    query = update.callback_query
    if not query:
        return

    current = bool(context.user_data.get("ignore_cooldown"))
    context.user_data["ignore_cooldown"] = not current

    state = context.chat_data.get(SESSION_KEY)
    extra_rows = _after_parse_extra_rows(state)
    user = getattr(query, "from_user", None)
    is_admin = bool(user and user.id in ADMIN_IDS)
    markup = build_after_parse_combined_kb(
        extra_rows=extra_rows,
        is_admin=is_admin,
        ignore_cooldown=bool(context.user_data.get("ignore_cooldown")),
    )
    try:
        await query.edit_message_reply_markup(reply_markup=markup)
    except BadRequest:
        pass

    status = "включён" if context.user_data.get("ignore_cooldown") else "выключен"
    try:
        await query.answer(f"Режим «Игнорировать 180 дней»: {status}")
    except BadRequest:
        # Fallback to posting a message if answering fails
        await query.message.reply_text(
            f"Режим «Игнорировать 180 дней (ручная)» теперь {status}."
        )


async def open_dirs_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Display the direction selection menu from an inline button."""

    query = update.callback_query
    if not query:
        return
    await query.answer()
    state = context.chat_data.get(SESSION_KEY)
    selected = getattr(state, "group", None) if state else None
    await _send_direction_prompt(query.message, context, selected=selected)


async def bulk_edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Enter the bulk e-mail editing flow."""

    query = update.callback_query
    await query.answer()
    if not ENABLE_INLINE_EMAIL_EDITOR:
        await query.message.reply_text(
            "Редактор в чате отключён. Используйте:\n"
            "• ✏️ Отправить правки текстом (замены «старый -> новый» и адреса для удаления)\n"
        )
        return

    previous = context.user_data.get("bulk_edit_message")
    if previous:
        try:
            await context.bot.delete_message(
                chat_id=previous[0], message_id=previous[1]
            )
        except Exception:
            pass

    state = get_state(context)
    working = _unique_preserve_order(state.to_send)
    context.user_data["bulk_edit_working"] = working
    context.user_data["bulk_edit_mode"] = None
    context.user_data["bulk_edit_page"] = 0
    context.user_data.pop("bulk_edit_replace_old", None)

    text = _bulk_edit_status_text(context)
    markup = build_bulk_edit_kb(
        working, page=0, page_size=BULK_EDIT_PAGE_SIZE
    )
    message = await query.message.reply_text(text, reply_markup=markup)
    context.user_data["bulk_edit_message"] = (message.chat_id, message.message_id)


async def bulk_edit_add_prompt(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Ask the user to provide additional e-mail addresses."""

    query = update.callback_query
    if not ENABLE_INLINE_EMAIL_EDITOR:
        await query.answer()
        return
    await query.answer()
    context.user_data["bulk_edit_mode"] = "add"
    context.user_data.pop("bulk_edit_replace_old", None)
    await query.message.reply_text("Отправьте адрес(а) через запятую.")


async def bulk_edit_replace_prompt(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Ask for the address that should be replaced."""

    query = update.callback_query
    if not ENABLE_INLINE_EMAIL_EDITOR:
        await query.answer()
        return
    await query.answer()
    context.user_data["bulk_edit_mode"] = "replace_wait_old"
    context.user_data.pop("bulk_edit_replace_old", None)
    await query.message.reply_text("Укажите адрес, который нужно заменить.")


async def bulk_edit_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove a single e-mail from the working list."""

    query = update.callback_query
    if not ENABLE_INLINE_EMAIL_EDITOR:
        await query.answer()
        return
    data = (query.data or "").strip()
    action, payload = _split_cb(data)
    if action != "bulk":
        await query.answer()
        return
    section, rest = _split_cb(payload)
    if section != "edit":
        await query.answer()
        return
    op, target = _split_cb(rest)
    if op != "del" or not target:
        await query.answer(cache_time=0, text="Некорректная команда.")
        return
    await query.answer("Удалено")
    working = [
        item for item in context.user_data.get("bulk_edit_working", []) if item != target
    ]
    context.user_data["bulk_edit_working"] = working
    current_page = context.user_data.get("bulk_edit_page", 0)
    if working:
        max_page = max((len(working) - 1) // BULK_EDIT_PAGE_SIZE, 0)
        context.user_data["bulk_edit_page"] = min(int(current_page), max_page)
    else:
        context.user_data["bulk_edit_page"] = 0
    await _update_bulk_edit_message(context, "Адрес удалён.")


async def bulk_edit_page(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Switch between pages in the bulk edit keyboard."""

    query = update.callback_query
    if not ENABLE_INLINE_EMAIL_EDITOR:
        await query.answer()
        return
    await query.answer()
    data = (query.data or "").strip()
    action, payload = _split_cb(data)
    if action != "bulk":
        return
    section, rest = _split_cb(payload)
    if section != "edit":
        return
    op, raw_page = _split_cb(rest)
    if op != "page" or not raw_page:
        return
    try:
        page = int(raw_page)
    except ValueError:
        return
    context.user_data["bulk_edit_page"] = page
    await _update_bulk_edit_message(context)


async def bulk_edit_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Finalize the edited list and return to group selection."""

    query = update.callback_query
    if not ENABLE_INLINE_EMAIL_EDITOR:
        await query.answer()
        return
    await query.answer()

    working = list(context.user_data.get("bulk_edit_working", []))
    filtered = [email for email in working if is_allowed_tld(email)]
    unique = _unique_preserve_order(filtered)

    state = get_state(context)
    state.to_send = unique
    state.preview_allowed_all = list(unique)
    state.suspect_numeric = sorted(
        {email for email in unique if is_numeric_localpart(email)}
    )
    state.foreign = []
    state.blocked_after_parse = count_blocked(state.to_send)

    context.user_data["last_parsed_emails"] = list(unique)

    context.user_data["bulk_edit_working"] = unique
    context.user_data["bulk_edit_page"] = 0
    await _update_bulk_edit_message(
        context,
        "Редактирование завершено.",
        disable_markup=True,
    )

    context.user_data.pop("bulk_edit_message", None)
    context.user_data.pop("bulk_edit_mode", None)
    context.user_data.pop("bulk_edit_replace_old", None)
    context.user_data.pop("bulk_edit_working", None)
    context.user_data.pop("bulk_edit_page", None)

    state = context.chat_data.get(SESSION_KEY)
    selected = getattr(state, "group", None) if state else None
    await _send_direction_prompt(query.message, context, selected=selected)


async def prompt_mass_send(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Compatibility wrapper for the bulk send start callback."""

    await send_all(update, context)


def _audit_append_correction(
    user_id: int,
    old_raw: str,
    old_norm: str,
    new_raw: str,
    new_norm: str,
    note: str = "",
) -> None:
    """Append correction info to audit CSV."""

    path = Path("var/audit_corrections.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        if not exists:
            writer.writerow(
                ["ts", "user_id", "old_raw", "old_norm", "new_raw", "new_norm", "note"]
            )
        writer.writerow(
            [
                datetime.utcnow().isoformat(),
                user_id,
                old_raw,
                old_norm,
                new_raw,
                new_norm,
                note,
            ]
        )


def _parse_corrections(text: str) -> tuple[list[tuple[str, str]], set[str]]:
    """Parse replacements and deletions from free-form text edits."""

    if not text:
        return [], set()

    cleaned = text.replace("→", "->").replace("=>", "->")
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    pairs: list[tuple[str, str]] = []
    to_delete: set[str] = set()

    for line in lines:
        lowered = line.lower()
        if lowered.startswith("- ") or lowered.startswith("— ") or lowered.startswith("удалить:"):
            for email in _extract_emails_loose(line):
                to_delete.add(email)
            continue
        # try different syntaxes for replacements first
        if "->" in line:
            parts = [part.strip() for part in line.split("->") if part.strip()]
            if len(parts) >= 2:
                old = parts[0]
                new = "->".join(parts[1:]).strip()
                if old and new:
                    pairs.append((old, new))
                continue

        if ":" in line and "," not in line:
            left, right = [part.strip() for part in line.split(":", 1)]
            if left and right:
                pairs.append((left, right))
                continue

        if "," in line:
            parts = [part.strip() for part in line.split(",") if part.strip()]
            if len(parts) == 2:
                pairs.append((parts[0], parts[1]))
                continue
            for idx in range(0, len(parts) - 1, 2):
                first = parts[idx]
                second = parts[idx + 1]
                if first and second:
                    pairs.append((first, second))
            if len(parts) >= 2:
                continue

        tokens = line.split()
        if len(tokens) >= 2:
            old = tokens[0].strip()
            new = " ".join(tokens[1:]).strip()
            if old and new:
                pairs.append((old, new))
                continue

        # no replacement detected — treat as deletion request
        for email in _extract_emails_loose(line):
            to_delete.add(email)

    # also extract emails from the whole text to catch space/comma separated lists
    all_emails = set(_extract_emails_loose(cleaned))
    old_emails = {old for old, _ in pairs}
    new_emails = {new for _, new in pairs}
    to_delete |= all_emails - old_emails - new_emails

    return pairs, to_delete


async def bulk_txt_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Включить режим ожидания текстовых правок адресов."""

    query = update.callback_query
    await query.answer()

    emails = context.user_data.get("last_parsed_emails") or []
    if not emails:
        state = get_state(context)
        emails = list(state.to_send or [])
        if emails:
            context.user_data["last_parsed_emails"] = emails

    if not emails:
        await query.message.reply_text("Список пуст — сначала выполните парсинг.")
        return

    context.user_data["awaiting_corrections_text"] = True
    await query.message.reply_text(
        "Режим правок включён. Пришлите одним сообщением адреса и/или замены.\n"
        "• Замены: формат «старый -> новый», по одной паре в строке.\n"
        "• Можно прислать несколько строк."
    )


async def bulk_delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запросить список адресов для удаления из подготовленного списка."""

    query = update.callback_query
    await query.answer()

    emails = context.user_data.get("last_parsed_emails") or []
    if not emails:
        state = get_state(context)
        emails = list(state.to_send or [])
        if emails:
            context.user_data["last_parsed_emails"] = emails

    if not emails:
        await query.message.reply_text("Список пуст — сначала выполните парсинг.")
        return

    user_id = update.effective_user.id if update.effective_user else "?"
    logger.info("bulk_delete: entered by user %s", user_id)
    context.user_data["awaiting_corrections_text"] = False
    await query.message.reply_text(
        "Вставьте адреса для удаления (через пробел, запятую, точку с запятой или с новой строки)."
    )

    return BULK_DELETE


async def bulk_delete_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Удалить указанные адреса из текущего списка рассылки."""

    message = update.message
    if not message:
        return ConversationHandler.END

    text = (message.text or "").strip()
    logger.info(
        "bulk_delete: processing text input (len=%d)",
        len(text),
    )

    if not text:
        await message.reply_text(
            "Не нашла корректных адресов. Пришлите ещё раз (через пробелы, запятые или строки)."
        )
        return ConversationHandler.END

    tokens = [part.strip() for part in re.split(r"[,;\s]+", text) if part.strip()]
    normalized: list[str] = []
    invalid: list[str] = []
    seen_norm: set[str] = set()
    for token in tokens:
        normalized_email = normalize_email(token)
        if normalized_email:
            key = normalized_email.lower()
            if key not in seen_norm:
                normalized.append(normalized_email)
                seen_norm.add(key)
        else:
            invalid.append(token)

    if not normalized:
        if invalid:
            await message.reply_text(
                "Не удалось распознать адреса. Проверьте формат и попробуйте ещё раз."
            )
        else:
            await message.reply_text("Не нашла корректных адресов. Попробуйте ещё раз.")
        return ConversationHandler.END

    state = get_state(context)
    current = list(context.user_data.get("last_parsed_emails") or state.to_send or [])
    if not current:
        await message.reply_text("Список пуст — удалять нечего.")
        return ConversationHandler.END

    current_lower = [item.lower() for item in current]
    to_remove = {email.lower() for email in normalized if email.lower() in current_lower}
    missing = [email for email in normalized if email.lower() not in to_remove]

    if not to_remove:
        reply_parts = ["Не нашла указанные адреса в текущем списке."]
        if missing:
            sample = ", ".join(missing[:6])
            reply_parts.append(f"Примеры: {sample}")
        if invalid:
            sample_invalid = ", ".join(invalid[:6])
            reply_parts.append(f"Некорректные записи: {sample_invalid}")
        await message.reply_text("\n".join(reply_parts))
        return ConversationHandler.END

    updated = [item for item in current if item.lower() not in to_remove]
    removed = len(current) - len(updated)

    context.user_data["last_parsed_emails"] = list(updated)
    context.user_data["bulk_edit_working"] = list(updated)
    context.user_data["awaiting_corrections_text"] = False
    _clamp_bulk_edit_page(context)

    state.to_send = list(updated)
    state.preview_allowed_all = list(updated)
    state.suspect_numeric = sorted(
        {email for email in updated if is_numeric_localpart(email)}
    )
    state.foreign = []
    state.blocked_after_parse = count_blocked(state.to_send)

    blocked_now = state.blocked_after_parse

    context.chat_data.pop("bulk_handler", None)

    reply_lines = [
        f"🗑 Удалено: {removed}. Осталось: {len(updated)}.",
        f"🚫 В стоп-листе (по текущему списку): {blocked_now}",
    ]

    if missing:
        reply_lines.append(
            f"Не нашла в текущем списке: {len(missing)}. Примеры: {', '.join(missing[:6])}"
        )
    if invalid:
        reply_lines.append(
            f"Некорректных записей: {len(invalid)}. Примеры: {', '.join(invalid[:6])}"
        )

    await message.reply_text("\n".join(reply_lines))

    try:
        await _update_bulk_edit_message(context, "Список обновлён.")
    except Exception:
        pass

    try:
        await prompt_change_group(update, context)
    except Exception:
        pass

    return ConversationHandler.END


async def corrections_text_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Принять текстовые правки адресов от пользователя."""

    awaiting_corrections = bool(context.user_data.get("awaiting_corrections_text"))
    if not awaiting_corrections:
        return

    message = update.message
    if not message:
        return

    text = (message.text or "").strip()

    pairs, to_delete_raw = _parse_corrections(text)
    if not pairs and not to_delete_raw:
        await message.reply_text(
            "Не распознаны правки. Используйте формат «старый -> новый» или перечислите адреса."
        )
        return

    raw_last = context.user_data.get("last_parsed_emails") or []
    if not raw_last:
        state = get_state(context)
        raw_last = list(state.to_send or [])
    last_parsed = list(raw_last)
    last_set = set(last_parsed)

    accepted_new: list[str] = []
    removed_by_replace = 0
    removed_direct = 0
    invalid_rows: list[tuple[str, str]] = []
    invalid_deletions: list[str] = []
    missing_deletions: list[str] = []

    user_id = update.effective_user.id if update.effective_user else 0

    for old_raw, new_raw in pairs:
        old_clean, _ = sanitize_email(old_raw)
        new_clean, _ = sanitize_email(new_raw)
        if not new_clean:
            invalid_rows.append((old_raw, new_raw))
            _audit_append_correction(
                user_id, old_raw, old_clean, new_raw, new_clean, "new_invalid"
            )
            continue

        accepted_new.append(new_clean)
        if old_clean and old_clean in last_set:
            try:
                last_parsed.remove(old_clean)
                last_set.remove(old_clean)
                removed_by_replace += 1
            except ValueError:
                pass

        _audit_append_correction(
            user_id,
            old_raw,
            old_clean,
            new_raw,
            new_clean,
            "mapped" if old_clean else "added",
        )

    if to_delete_raw:
        for raw_email in sorted(to_delete_raw):
            clean_email, _ = sanitize_email(raw_email)
            if not clean_email:
                invalid_deletions.append(raw_email)
                continue
            if clean_email in last_set:
                try:
                    last_parsed.remove(clean_email)
                    last_set.remove(clean_email)
                    removed_direct += 1
                except ValueError:
                    pass
            else:
                missing_deletions.append(raw_email)

    final = sorted(set(last_parsed) | set(accepted_new))

    context.user_data["last_parsed_emails"] = final
    context.user_data["awaiting_corrections_text"] = False

    state = get_state(context)
    state.to_send = final
    state.preview_allowed_all = list(final)
    state.suspect_numeric = sorted(
        {email for email in final if is_numeric_localpart(email)}
    )
    state.foreign = []
    state.blocked_after_parse = count_blocked(state.to_send)
    context.chat_data.pop("bulk_handler", None)

    total_removed = removed_by_replace + removed_direct
    summary_lines = [
        f"🔁 Замен: {len(pairs)}",
        f"➕ Добавлено новых адресов: {len(set(accepted_new))}",
        f"🗑 Удалено адресов: {total_removed}",
        f"📦 Итоговый размер списка: {len(final)}",
    ]

    if to_delete_raw:
        summary_lines.append(
            f"   • Запрошено к удалению: {len(to_delete_raw)}, удалено: {removed_direct}"
        )

    if invalid_rows:
        sample = ", ".join(f"{old}->{new}" for old, new in invalid_rows[:6])
        summary_lines.append(
            f"Невалидных пар: {len(invalid_rows)}. Примеры: {sample}"
        )

    if invalid_deletions:
        sample = ", ".join(invalid_deletions[:6])
        summary_lines.append(
            f"Невалидных адресов для удаления: {len(invalid_deletions)}. Примеры: {sample}"
        )

    if missing_deletions:
        sample = ", ".join(missing_deletions[:6])
        summary_lines.append(
            f"Не найдено в текущем списке: {len(missing_deletions)}. Примеры: {sample}"
        )

    await message.reply_text("\n".join(summary_lines))

    try:
        await prompt_change_group(update, context)
    except Exception:
        await message.reply_text("Готово. Теперь выберите направление рассылки.")


async def select_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle group selection and prepare messages for sending."""

    query = update.callback_query
    raw_data = (query.data or "").strip()
    prefix, payload = _split_direction_callback(raw_data)
    group_key = payload or raw_data
    group_code_norm = _normalize_template_code(group_key)
    if not group_code_norm:
        await query.answer(
            cache_time=0,
            text="Некорректное направление. Обновите меню и попробуйте снова.",
            show_alert=True,
        )
        return
    template_info = get_template_from_map(context, prefix or "dir:", group_key)
    label = groups_map.get(group_code_norm, group_code_norm)
    signature = ""
    template_code = group_code_norm
    if template_info:
        raw_code = str(template_info.get("code") or "").strip()
        if raw_code:
            template_code = raw_code
            normalized = _normalize_template_code(raw_code)
            if normalized:
                group_code_norm = normalized
        meta_label = _template_label(template_info)
        if meta_label:
            label = meta_label
        raw_signature = template_info.get("signature")
        if isinstance(raw_signature, str) and raw_signature.strip():
            signature = raw_signature.strip()

    template_path = None
    info = template_info or get_template(template_code)
    if info:
        raw_path = info.get("path") if isinstance(info, dict) else None
        if isinstance(raw_path, str) and raw_path.strip():
            template_path = raw_path.strip()
        if not label:
            inferred = _template_label(info)
            if inferred:
                label = inferred
    if not template_path:
        template_path = (
            TEMPLATE_MAP.get(group_code_norm)
            or TEMPLATE_MAP.get(template_code)
        )
    if not template_path:
        await query.answer(
            cache_time=0,
            text="Шаблон не найден. Обновите меню и попробуйте снова.",
            show_alert=True,
        )
        return
    path_obj = Path(template_path)
    if not path_obj.exists():
        await query.answer(
            cache_time=0,
            text="Файл шаблона не найден. Обновите меню и попробуйте снова.",
            show_alert=True,
        )
        return
    template_label = (
        get_template_label(template_code)
        or label
        or template_code
    )
    template_path_str = str(path_obj)
    state = get_state(context)
    # Нормализуем источник адресов после возможных правок/предпросмотра:
    emails = state.to_send or []
    if not emails:
        fallback = context.user_data.get("last_parsed_emails")
        if isinstance(fallback, list):
            emails = fallback
            state.to_send = fallback
    if not isinstance(emails, list):
        emails = list(emails)
    emails = [str(item).strip() for item in emails if str(item).strip()]
    state.to_send = emails
    state.blocked_after_parse = count_blocked(state.to_send)
    if not emails:
        await query.answer(
            cache_time=0,
            text=(
                "Список адресов пуст. Сначала выполните парсинг или внесите правки, "
                "затем повторите выбор направления."
            ),
            show_alert=True,
        )
        return
    state.group = group_code_norm
    state.template = template_path_str
    state.template_label = template_label
    context.user_data["selected_dir"] = group_code_norm
    if signature:
        context.user_data["selected_signature"] = signature
    else:
        context.user_data.pop("selected_signature", None)
    context.chat_data["current_template_code"] = group_code_norm
    context.chat_data["current_template_label"] = template_label
    context.chat_data["current_template_path"] = template_path_str
    markup = _build_group_markup(
        context,
        prefix=prefix or "dir:",
        selected=group_code_norm,
    )
    # Обновляем клавиатуру устойчиво: при любых проблемах — тихий фоллбэк в новый месседж
    try:
        await query.edit_message_reply_markup(reply_markup=markup)
    except BadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            try:
                await query.message.reply_text(
                    "⬇️ Выберите направление рассылки:", reply_markup=markup
                )
            except Exception:
                pass
    except Exception:
        try:
            await query.message.reply_text(
                "⬇️ Выберите направление рассылки:", reply_markup=markup
            )
        except Exception:
            pass
    await query.answer(f"Выбрано: {label}")
    chat_id = query.message.chat.id
    # Гарантированно не роняемся на неожиданных проблемах подготовки очереди
    try:
        ignore_cooldown = _is_ignore_cooldown_enabled(context)
        ready, blocked_foreign, blocked_invalid, skipped_recent, digest = (
            messaging.prepare_mass_mailing(
                emails,
                group_code_norm,
                ignore_cooldown=ignore_cooldown,
            )
        )
        expected_ready = getattr(state, "preview_ready_count", None)
        if isinstance(expected_ready, int) and expected_ready >= 0:
            assert len(ready) == expected_ready, (
                f"Mismatch ready_count({expected_ready}) vs emails_to_send({len(ready)})"
            )
    except Exception as exc:
        logger.exception(
            "prepare_mass_mailing failed",
            extra={
                "event": "select_group",
                "code": group_code_norm,
                "phase": "prepare",
            },
        )
        await query.message.reply_text(
            "⚠️ Не удалось подготовить список к рассылке. "
            "Обновите меню или повторите выбор направления.",
            reply_markup=markup,
        )
        return
    log_mass_filter_digest(
        {
            **digest,
            "batch_id": context.chat_data.get("batch_id"),
            "chat_id": chat_id,
            "entry_url": context.chat_data.get("entry_url"),
            "template_label": template_label,
        }
    )
    state.to_send = ready
    state.blocked_after_parse = count_blocked(state.to_send)
    state.override_cooldown = _is_ignore_cooldown_enabled(context)
    if isinstance(digest, dict):
        context.chat_data["last_digest"] = dict(digest)
    else:
        context.chat_data["last_digest"] = {}
    state.last_digest = _snapshot_mass_digest(
        digest,
        ready_after_cooldown=(
            int(digest.get("ready_after_cooldown"))
            if isinstance(digest, dict) and digest.get("ready_after_cooldown") is not None
            else None
        ),
        ready_final=len(ready),
    )
    active_norms: set[str] = set()
    if ready:
        for addr in ready:
            norm = normalize_email(addr) or str(addr).strip().lower()
            if norm:
                active_norms.add(norm)
    filtered_sources = {}
    if getattr(state, "source_map", None):
        try:
            filtered_sources = {
                norm: list(state.source_map.get(norm, []))
                for norm in active_norms
                if norm in state.source_map
            }
        except Exception:
            filtered_sources = {}

    mass_state.save_chat_state(
        chat_id,
        {
            "group": group_code_norm,
            "template": template_path_str,
            "template_label": template_label,
            "pending": ready,
            "blocked_foreign": blocked_foreign,
            "blocked_invalid": blocked_invalid,
            "skipped_recent": skipped_recent,
            "batch_id": context.chat_data.get("batch_id"),
            "source_map": filtered_sources,
        },
    )
    handler_payload = {
        "emails": list(ready),
        "group": group_code_norm,
        "chat_id": chat_id,
        "template": template_path_str,
        "digest": digest,
    }
    context.chat_data["bulk_handler"] = handler_payload
    batch_id = _store_bulk_queue(
        context,
        group=group_code_norm,
        emails=ready,
        chat_id=chat_id,
        digest=digest,
        template=template_path_str,
    )
    if batch_id:
        handler_payload["batch_id"] = batch_id
    summary_payload = _store_mass_summary(
        chat_id,
        group=group_code_norm,
        ready=ready,
        blocked_foreign=blocked_foreign,
        blocked_invalid=blocked_invalid,
        skipped_recent=skipped_recent,
        digest=digest,
        total_incoming=len(emails),
    )
    await _maybe_send_skipped_summary(query, summary_payload)
    if not ready:
        await query.message.reply_text(
            "Все адреса уже в истории за 180 дней или в стоп-листах.",
            reply_markup=None,
        )
        return
    if batch_id:
        keyboard = None
        try:  # pragma: no cover - optional legacy dependency
            from emailbot import telegram_ui  # type: ignore

            build_keyboard = getattr(telegram_ui, "build_mass_preview_keyboard", None)
            if callable(build_keyboard):
                keyboard = build_keyboard(batch_id)
        except Exception:
            keyboard = None
    else:
        keyboard = None

    if keyboard is None and batch_id:
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🚀 Начать рассылку",
                        callback_data=f"bulk_start:{batch_id}",
                    )
                ]
            ]
        )

    await query.message.reply_text(
        (
            f"✉️ Готово к отправке {len(ready)} писем.\n"
            "Для запуска рассылки нажмите кнопку ниже."
        ),
        reply_markup=keyboard,
    )


async def manual_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the inline "Ручная" button press."""

    query = update.callback_query
    await query.answer()
    context.chat_data["awaiting_manual_emails"] = True
    context.chat_data["manual_emails"] = []
    context.chat_data["manual_group"] = None
    context.user_data["awaiting_manual_email"] = True
    context.user_data.pop("manual_emails", None)
    context.user_data.pop("text_corrections", None)
    context.user_data["ignore_180d"] = False
    await query.message.reply_text(
        "Введите email или список email-адресов (через запятую/пробел/с новой строки):"
    )


async def manual_input_router(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Route manual input messages through the text handler and stop processing."""

    if context.user_data.get("state") != MANUAL_WAIT_INPUT:
        return

    message = update.message
    if not message:
        return

    raw_text = message.text or ""
    text = raw_text.strip()
    if not text:
        raise ApplicationHandlerStop

    # EBOT-MANUAL-NOURL-WHEN-EMAILS: если в тексте есть хотя бы один символ '@',
    # считаем, что пользователь прислал адреса, даже если встречаются доменные
    # шаблоны. Это защищает от ложного срабатывания URL-ветки, где мы запрещаем
    # ссылки в ручном режиме.
    looks_like_emails = "@" in raw_text

    if not looks_like_emails and _message_has_url(message, message.text):
        await message.reply_text(MANUAL_URL_REJECT_MESSAGE)
        raise ApplicationHandlerStop

    raw_emails = messaging.parse_emails_from_text(text)
    if not raw_emails:
        await message.reply_text(
            "Не нашла корректных адресов. Пришлите ещё раз (допустимы запятая/пробел/новая строка)."
        )
        context.user_data.pop("state", None)
        context.user_data["awaiting_manual_email"] = False
        raise ApplicationHandlerStop

    allowed: list[str] = []
    dropped: list[tuple[str, str]] = []
    for email in raw_emails:
        normalized, drop_reason = _classify_manual_email(email)
        if drop_reason:
            dropped.append((email, drop_reason))
            continue
        if normalized:
            allowed.append(normalized)

    stored = _update_manual_storage(context, allowed)
    context.chat_data["manual_drop_reasons"] = dropped
    context.chat_data["manual_group"] = None
    context.chat_data["awaiting_manual_emails"] = False
    context.user_data["manual_emails"] = stored
    context.user_data["awaiting_manual_email"] = False
    context.user_data.pop("state", None)

    preview_lines = [
        "✅ Ручная отправка — предпросмотр",
        f"Всего получено: {len(raw_emails)}",
        f"К отправке: {len(stored)}",
    ]
    if dropped:
        preview_lines.append(f"Исключено: {len(dropped)}")
    await message.reply_text("\n".join(preview_lines))

    await _send_manual_summary(update, context, stored, dropped)
    raise ApplicationHandlerStop


async def route_text_message(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Universal router for plain text updates."""

    message = update.message
    if message is None:
        return

    raw_text = message.text or ""
    text = raw_text.strip()

    urls: list[str] = []
    entities = getattr(message, "entities", None)
    if entities:
        for ent in entities:
            if ent.type == "url":
                segment = raw_text[ent.offset : ent.offset + ent.length].strip()
                if segment:
                    urls.append(segment)
            elif ent.type == "text_link" and getattr(ent, "url", None):
                urls.append(ent.url)
    if not urls and text:
        urls = [
            item.rstrip(".,;:!?)]}'\"")
            for item in URL_REGEX.findall(text)
            if item
        ]
    if urls:
        awaiting_manual = bool(
            context.chat_data.get("awaiting_manual_emails")
            or context.user_data.get("awaiting_manual_email")
        )
        if awaiting_manual:
            await message.reply_text(MANUAL_URL_REJECT_MESSAGE)
            raise ApplicationHandlerStop
        await handle_url_text(update, context)
        raise ApplicationHandlerStop

    awaiting = context.chat_data.get("awaiting_manual_emails") or context.user_data.get(
        "awaiting_manual_email"
    )
    if not awaiting:
        return
    if context.user_data.get("text_corrections"):
        return

    if (
        not text
        or text in {"✉️ Ручная", "Ручная"}
        or text.startswith("✉️")
    ):
        raise ApplicationHandlerStop

    emails = messaging.parse_emails_from_text(text)
    if not emails:
        await message.reply_text(
            "Не нашла корректных адресов. Пришлите ещё раз (допустимы запятая/пробел/новая строка)."
        )
        raise ApplicationHandlerStop

    context.chat_data["manual_emails"] = emails
    context.chat_data["manual_group"] = None
    context.chat_data["awaiting_manual_emails"] = False
    context.user_data["manual_emails"] = emails
    context.user_data["awaiting_manual_email"] = False
    context.chat_data["manual_drop_reasons"] = []

    status = _cooldown_status(context)
    await message.reply_text(
        (
            f"Принято адресов: {len(emails)}\n"
            "Теперь выберите направление.\n"
            f"Правило 180 дней: {status}."
        ),
        reply_markup=_group_keyboard(context, prefix="manual_group_"),
    )

    raise ApplicationHandlerStop


async def enable_text_corrections(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Enable manual list text corrections mode."""

    query = update.callback_query
    if not query:
        return

    manual_emails = (
        context.user_data.get("manual_emails")
        or context.chat_data.get("manual_emails")
        or []
    )
    if not manual_emails:
        await query.answer(show_alert=True, text="Сначала введите адреса для ручной рассылки.")
        return

    await query.answer()
    message = query.message
    if not message:
        return
    context.user_data["text_corrections"] = True
    context.user_data["awaiting_manual_email"] = True
    context.user_data["state"] = MANUAL_WAIT_INPUT

    await message.reply_text(
        (
            "✏️ Режим текстовых правок включён.\n"
            "• Замены: формат «старый -> новый», по одному на строку.\n"
            "• Удаление: строка вида «- addr1, addr2» или «Удалить: addr1; addr2».\n"
            "• Перечислите адреса, чтобы заменить список целиком."
        )
    )


async def toggle_ignore_180d(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Toggle manual send 180-day cooldown bypass."""

    query = update.callback_query
    if not query:
        return

    state = get_state(context)
    current = bool(context.user_data.get("ignore_180d"))
    new_value = not current
    context.user_data["ignore_180d"] = new_value
    context.user_data["ignore_cooldown"] = new_value
    state.override_cooldown = new_value
    status = _cooldown_status(context)

    manual_group = context.chat_data.get("manual_group")
    manual_emails = (
        context.chat_data.get("manual_emails")
        or context.user_data.get("manual_emails")
        or []
    )

    markup = _group_keyboard(
        context,
        prefix="manual_group_",
        selected=manual_group,
    )

    message = query.message
    updated = False
    if message:
        new_text: str | None = None
        if manual_emails:
            new_text = (
                f"Принято адресов: {len(manual_emails)}\n"
                "Теперь выберите направление.\n"
                f"Правило 180 дней: {status}."
            )
        else:
            text = message.text or ""
            if "Правило 180 дней" in text:
                prefix, _, suffix = text.partition("Правило 180 дней")
                _, dot, tail = suffix.partition(".")
                tail = tail.lstrip("\n") if dot else suffix.lstrip("\n")
                new_text = f"{prefix}Правило 180 дней: {status}."
                if tail:
                    new_text += f"\n{tail}"
        if new_text:
            try:
                await message.edit_text(new_text, reply_markup=markup)
                updated = True
            except BadRequest:
                pass
        if not updated:
            try:
                await query.edit_message_reply_markup(reply_markup=markup)
                updated = True
            except BadRequest:
                pass

    try:
        await query.answer(f"Правило 180 дней: {status}")
    except BadRequest:
        if message:
            try:
                await message.reply_text(f"⚠️ Правило 180 дней: {status}.")
            except Exception:
                pass
        return

    if not updated and message:
        try:
            await message.reply_text(f"⚠️ Правило 180 дней: {status}.")
        except Exception:
            pass

    try:
        await _update_bulk_edit_message(context)
    except Exception:
        pass

    digest_snapshot = _snapshot_mass_digest(
        state.last_digest,
        ready_after_cooldown=len(state.to_send),
        ready_final=len(state.to_send),
    )
    if new_value:
        planned = digest_snapshot.get("ready_final", 0)
    else:
        planned = digest_snapshot.get("ready_after_cooldown", 0)
    summary_text = (
        f"🚀 Режим игнора лимита 180 дней {'включен' if new_value else 'выключен'}. "
        f"Ожидаемых отправок с учётом фильтров: ~{planned}."
    )
    if message:
        try:
            await message.reply_text(summary_text)
        except Exception:
            pass
    else:
        try:
            await query.message.reply_text(summary_text)
        except Exception:
            pass

async def _send_batch_with_sessions(
    query: CallbackQuery,
    context: ContextTypes.DEFAULT_TYPE,
    recipients: list[str],
    template_path: str,
    group_code: str,
    *,
    ignore_cooldown: bool = False,
) -> None:
    """Send e-mails using the resilient session-aware pipeline."""

    chat_id = query.message.chat.id
    to_send = list(dict.fromkeys(recipients))
    if not to_send:
        await query.message.reply_text(
            "Никого не осталось для отправки по правилам (фильтры/полугодовой лимит)."
        )
        return

    sent_today = get_sent_today()
    available = max(0, MAX_EMAILS_PER_DAY - len(sent_today))
    if available <= 0 and not is_force_send(chat_id):
        logger.info(
            "Daily limit reached: %s emails sent today (source=sent_log)",
            len(sent_today),
        )
        await query.message.reply_text(
            (
                f"❗ Дневной лимит {MAX_EMAILS_PER_DAY} уже исчерпан.\n"
                "Если вы исправили ошибки — нажмите «🚀 Игнорировать лимит» и запустите ещё раз."
            )
        )
        return

    if not is_force_send(chat_id) and len(to_send) > available:
        to_send = to_send[:available]
        await query.message.reply_text(
            (
                f"⚠️ Учитываю дневной лимит: будет отправлено "
                f"{available} адресов из списка."
            )
        )

    attempt_total = len(to_send)

    await query.message.reply_text(
        f"✉️ Рассылка начата. Отправляем {attempt_total} писем..."
    )

    try:
        host = os.getenv("IMAP_HOST", "imap.mail.ru")
        port = int(os.getenv("IMAP_PORT", "993"))
        imap = imaplib.IMAP4_SSL(host, port)
        imap.login(messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD)
        sent_folder = get_preferred_sent_folder(imap)
        imap.select(f'"{sent_folder}"')
    except Exception as exc:
        log_error(f"imap connect: {exc}")
        await query.message.reply_text(f"❌ IMAP ошибка: {exc}")
        return

    error_details: list[str] = []
    duplicates: list[str] = []
    cancel_event = context.chat_data.get("cancel_event")
    host = os.getenv("SMTP_HOST", "smtp.mail.ru")
    port = int(os.getenv("SMTP_PORT", "465"))
    ssl_env = os.getenv("SMTP_SSL")
    use_ssl = None if not ssl_env else ssl_env == "1"
    retries = int(os.getenv("SMTP_CONNECT_RETRIES", "3"))
    backoff = float(os.getenv("SMTP_CONNECT_BACKOFF", "1.0"))

    import smtplib

    sent_count = 0
    aborted = False
    attempt = 0
    total = len(to_send)
    processed = 0
    last_progress_notice = 0
    while True:
        try:
            with SmtpClient(
                host,
                port,
                messaging.EMAIL_ADDRESS,
                messaging.EMAIL_PASSWORD,
                use_ssl=use_ssl,
            ) as client:
                while to_send:
                    if should_stop():
                        aborted = True
                        break
                    if cancel_event and cancel_event.is_set():
                        aborted = True
                        break
                    email_addr = to_send.pop(0)
                    processed += 1
                    try:
                        await heartbeat()
                        outcome, token, log_key, content_hash = send_email_with_sessions(
                            client,
                            imap,
                            sent_folder,
                            email_addr,
                            template_path,
                            subject=messaging.DEFAULT_SUBJECT,
                            override_180d=ignore_cooldown,
                        )
                        if outcome == messaging.SendOutcome.SENT:
                            log_sent_email(
                                email_addr,
                                group_code,
                                "ok",
                                chat_id,
                                template_path,
                                unsubscribe_token=token,
                                key=log_key,
                                subject=messaging.DEFAULT_SUBJECT,
                                content_hash=content_hash,
                            )
                            try:
                                mark_soft_bounce_success(email_addr)
                            except Exception:
                                pass
                            sent_count += 1
                            await asyncio.sleep(1.5)
                        elif outcome == messaging.SendOutcome.DUPLICATE:
                            duplicates.append(email_addr)
                            error_details.append("пропущено (дубль за 24 ч)")
                        elif outcome == messaging.SendOutcome.COOLDOWN:
                            error_details.append("пропущено (кулдаун 180 дней)")
                        elif outcome == messaging.SendOutcome.BLOCKED:
                            error_details.append("пропущено (стоп-лист)")
                        else:
                            error_details.append("ошибка отправки")
                    except messaging.TemplateRenderError as err:
                        missing = ", ".join(sorted(err.missing)) if err.missing else "—"
                        await context.bot.send_message(
                            chat_id=query.message.chat.id,
                            text=(
                                "⚠️ Шаблон не готов к отправке.\n"
                                f"Файл: {err.path}\n"
                                f"Не заполнены: {missing}\n\n"
                                "Подставь значения или создай рядом файл с текстом письма:\n"
                                "• <имя_шаблона>.body.txt — будет вставлен в {BODY}/{{BODY}}."
                            ),
                        )
                        try:
                            imap.logout()
                        except Exception:
                            pass
                        return
                    except Exception as err:
                        error_details.append(str(err))
                        code = getattr(err, "smtp_code", None)
                        msg_obj: object | None = None
                        if (
                            hasattr(err, "recipients")
                            and isinstance(err.recipients, dict)
                            and email_addr in err.recipients
                        ):
                            recipient_info = err.recipients[email_addr]
                            if isinstance(recipient_info, (list, tuple)) and recipient_info:
                                code = recipient_info[0]
                                msg_obj = recipient_info[1] if len(recipient_info) > 1 else None
                        if msg_obj is None:
                            msg_obj = getattr(err, "smtp_error", None)
                        if msg_obj is None and err.args:
                            msg_obj = err.args[0]
                        if isinstance(code, str):
                            try:
                                code = int(code)
                            except Exception:
                                pass
                        if isinstance(msg_obj, (bytes, bytearray)):
                            msg_text = msg_obj.decode("utf-8", "ignore")
                        else:
                            msg_text = str(msg_obj) if msg_obj is not None else ""
                        logger.error(
                            "SMTP error: code=%s msg=%s to=%s", code, msg_text, email_addr
                        )
                        try:
                            messaging.write_audit(
                                "smtp_error",
                                email=email_addr,
                                meta={"code": code, "message": msg_text},
                            )
                        except Exception:
                            logger.debug("smtp_error audit logging failed", exc_info=True)
                        add_bounce(email_addr, code, msg_text or str(err), phase="manual_send")
                        msg_for_classification = msg_obj if msg_obj is not None else msg_text
                        if is_hard_bounce(code, msg_for_classification):
                            suppress_add(email_addr, code, "hard bounce on send")
                        elif is_soft_bounce(code, msg_for_classification):
                            try:
                                code_int: Optional[int]
                                try:
                                    code_int = int(code) if code is not None else None
                                except Exception:
                                    code_int = None
                                if isinstance(msg_obj, (bytes, bytearray)):
                                    reason_text = msg_obj.decode("utf-8", "ignore")
                                else:
                                    reason_text = msg_text or str(err)
                                log_soft_bounce(
                                    email_addr,
                                    reason=reason_text,
                                    group_code=group_code,
                                    chat_id=chat_id,
                                    template_path=template_path,
                                    code=code_int,
                                )
                            except Exception:
                                pass
                        log_sent_email(
                            email_addr,
                            group_code,
                            "error",
                            chat_id,
                            template_path,
                            str(err),
                        )
                    if processed % 5 == 0 or processed == total:
                        logger.info("bulk_progress: %s/%s", processed, total)
                        await heartbeat()
                        if (
                            (processed % 20 == 0 or processed == total)
                            and processed != last_progress_notice
                        ):
                            try:
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"📬 Прогресс: {processed}/{total}",
                                )
                                last_progress_notice = processed
                            except Exception:
                                logger.debug(
                                    "bulk progress notification failed", exc_info=True
                                )
            break
        except (smtplib.SMTPServerDisconnected, TimeoutError, OSError) as exc:
            attempt += 1
            if attempt >= retries:
                logger.exception("SMTP connection retries exhausted", exc_info=exc)
                await query.message.reply_text(f"❌ SMTP ошибка: {exc}")
                try:
                    imap.logout()
                except Exception:
                    pass
                return
            await asyncio.sleep(backoff)
            backoff *= 2

    try:
        imap.logout()
    except Exception:
        pass

    if aborted:
        await query.message.reply_text(
            f"🛑 Остановлено. Отправлено писем: {sent_count}"
        )
    else:
        suffix = ""
        if sent_count == 0 and attempt_total > 0:
            suffix = (
                "\nℹ️ Проверьте: адреса могли попасть под стоп-лист, дубликаты,"
                " ограничения 180 дней или произошла SMTP-ошибка."
            )
        await query.message.reply_text(f"✅ Отправлено писем: {sent_count}{suffix}")
    if error_details:
        summary = format_error_details(error_details)
        if summary:
            await query.message.reply_text(summary)

    clear_recent_sent_cache()
    disable_force_send(chat_id)


async def manual_select_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Select a manual mailing group and start sending."""

    query = update.callback_query
    data = (query.data or "").strip()
    group_code = (
        data[len("manual_group_") :]
        if data.startswith("manual_group_")
        else data
    ).strip()
    await query.answer()
    chat_id = query.message.chat.id

    emails = (
        context.chat_data.get("manual_emails")
        or context.user_data.get("manual_emails")
        or []
    )
    if not emails:
        await query.message.reply_text("Сначала пришлите адреса текстом.")
        return

    context.chat_data["manual_group"] = group_code

    ignore_180d = bool(context.user_data.get("ignore_180d"))
    ready, blocked_foreign, blocked_invalid, skipped_recent, digest = (
        messaging.prepare_mass_mailing(
            list(emails),
            group_code,
            ignore_cooldown=ignore_180d,
        )
    )
    if digest.get("error"):
        logger.error(
            "prepare_mass_mailing failed (manual): %s",
            digest["error"],
            extra={"event": "manual", "code": group_code},
        )
        await query.message.reply_text(
            "⚠️ Не удалось подготовить список к ручной рассылке (проблема с журналом/данными). "
            "Попробуйте ещё раз или выберите другое направление."
        )
        return

    summary_payload = _store_mass_summary(
        chat_id,
        group=group_code,
        ready=ready,
        blocked_foreign=blocked_foreign,
        blocked_invalid=blocked_invalid,
        skipped_recent=skipped_recent,
        digest=digest,
        total_incoming=len(emails),
    )
    await _maybe_send_skipped_summary(query, summary_payload)

    logger.info(
        "manual prepare digest",
        extra={"event": "manual_prepare", "code": group_code, **digest},
    )

    summary_lines = []
    if ignore_180d:
        summary_lines.append("⚠️ Игнорировать правило 180 дней: ВКЛ")
    summary_lines.append(f"Будет отправлено: {len(ready)}")
    if blocked_foreign:
        summary_lines.append(f"🌍 Исключено иностранных доменов: {len(blocked_foreign)}")
    if blocked_invalid:
        summary_lines.append(f"🚫 Исключено заблокированных адресов: {len(blocked_invalid)}")
    if skipped_recent:
        summary_lines.append(f"🕓 Пропущено по лимиту 180 дней: {len(skipped_recent)}")
    if len(summary_lines) > 1:
        await query.message.reply_text("\n".join(summary_lines))

    if not ready:
        await query.message.reply_text(
            "Никого не осталось для отправки по правилам (фильтры/полугодовой лимит)."
        )
        return

    template_path = messaging.TEMPLATE_MAP.get(group_code)
    if not template_path or not Path(template_path).exists():
        await query.message.reply_text(
            "⚠️ Не найден шаблон письма для выбранного направления."
        )
        return

    await _send_batch_with_sessions(
        query,
        context,
        ready,
        template_path,
        group_code,
        ignore_cooldown=ignore_180d,
    )

    context.chat_data["awaiting_manual_emails"] = False
    context.chat_data["manual_emails"] = []
    context.chat_data["manual_group"] = None
    context.user_data.pop("manual_emails", None)
    context.user_data["awaiting_manual_email"] = False
    context.user_data.pop("text_corrections", None)


async def prompt_manual_email(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Ask the user to enter e-mail addresses manually."""

    clear_all_awaiting(context)
    context.user_data.pop("manual_emails", None)
    context.chat_data.pop("manual_all_emails", None)
    context.chat_data.pop("manual_drop_reasons", None)
    context.chat_data["manual_emails"] = []
    context.chat_data["manual_group"] = None
    context.chat_data["awaiting_manual_emails"] = True
    context.user_data["ignore_180d"] = False
    context.user_data.pop("text_corrections", None)
    await update.message.reply_text(
        (
            "Введите email или список email-адресов "
            "(через запятую/пробел/с новой строки):"
        )
    )
    context.user_data["awaiting_manual_email"] = True
    context.user_data["state"] = MANUAL_WAIT_INPUT


async def _handle_bulk_edit_text(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
) -> bool:
    """Process user replies for the bulk edit workflow."""

    mode = context.user_data.get("bulk_edit_mode")
    if not mode:
        return False

    if mode == "add":
        parts = [p.strip() for p in re.split(r"[,\s]+", text) if p.strip()]
        if not parts:
            await update.message.reply_text(
                "Не найдено адресов. Отправьте e-mail через запятую."
            )
            return True
        working = list(context.user_data.get("bulk_edit_working", []))
        valid: list[str] = []
        for item in parts:
            cleaned, _ = sanitize_email(item)
            if cleaned:
                valid.append(cleaned)
        if not valid:
            await update.message.reply_text(
                "Не удалось распознать корректные адреса, попробуйте ещё раз."
            )
            return True
        merged = _unique_preserve_order(working + valid)
        context.user_data["bulk_edit_working"] = merged
        context.user_data["bulk_edit_mode"] = None
        context.user_data["bulk_edit_page"] = max(
            0, (len(merged) - 1) // BULK_EDIT_PAGE_SIZE
        )
        skipped = len(parts) - len(valid)
        summary = [f"Добавлено: {len(valid)}"]
        if skipped:
            summary.append(f"Пропущено: {skipped}")
        summary.append(f"Текущий размер списка: {len(merged)}")
        message = ". ".join(summary)
        await update.message.reply_text(message)
        await _update_bulk_edit_message(context, message)
        return True

    if mode == "replace_wait_old":
        candidate = text.strip()
        working = list(context.user_data.get("bulk_edit_working", []))
        if not working:
            context.user_data["bulk_edit_mode"] = None
            await update.message.reply_text("Список пуст.")
            return True
        cleaned, _ = sanitize_email(candidate)
        if candidate in working:
            target = candidate
        elif cleaned and cleaned in working:
            target = cleaned
        else:
            await update.message.reply_text(
                "Адрес не найден в текущем списке. Укажите один адрес из списка."
            )
            return True
        context.user_data["bulk_edit_replace_old"] = target
        context.user_data["bulk_edit_mode"] = "replace_wait_new"
        await update.message.reply_text("Укажите новый адрес.")
        return True

    if mode == "replace_wait_new":
        old = context.user_data.get("bulk_edit_replace_old")
        if not old:
            context.user_data["bulk_edit_mode"] = None
            await update.message.reply_text(
                "Не выбран адрес для замены. Нажмите «🔁 Заменить» ещё раз."
            )
            return True
        cleaned, _ = sanitize_email(text)
        if not cleaned:
            await update.message.reply_text("Неверный e-mail, попробуйте ещё раз.")
            return True
        working = list(context.user_data.get("bulk_edit_working", []))
        try:
            idx = working.index(old)
        except ValueError:
            idx = None
        if idx is not None:
            working[idx] = cleaned
        else:
            working.append(cleaned)
            idx = len(working) - 1
        working = _unique_preserve_order(working)
        context.user_data["bulk_edit_working"] = working
        context.user_data["bulk_edit_mode"] = None
        context.user_data.pop("bulk_edit_replace_old", None)
        context.user_data["bulk_edit_page"] = max(0, idx // BULK_EDIT_PAGE_SIZE)
        await update.message.reply_text("Адрес заменён.")
        await _update_bulk_edit_message(context, "Адрес обновлён.")
        return True

    return False


async def handle_url_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    allow_manual: bool = False,
) -> None:
    """Handle text messages that contain URLs and extract e-mail addresses."""

    message = update.message
    if not message:
        return
    if context.user_data.get("awaiting_block_email") or context.user_data.get(
        "text_corrections"
    ):
        return
    if context.user_data.get("awaiting_manual_email") and not allow_manual:
        return

    raw_text = message.text or ""
    text = raw_text.strip()
    if not text:
        return

    urls: list[str] = []
    entities = getattr(message, "entities", None)
    if entities:
        for ent in entities:
            if ent.type == "url":
                segment = raw_text[ent.offset : ent.offset + ent.length].strip()
                if segment:
                    urls.append(segment)
            elif ent.type == "text_link" and getattr(ent, "url", None):
                urls.append(ent.url)
    if not urls:
        urls = [
            item.rstrip(".,;:!?)]}'\"")
            for item in URL_REGEX.findall(text)
            if item
        ]
    urls = [url for url in urls if url]
    if not urls:
        await message.reply_text("Не нашёл URL в сообщении 🤔")
        return

    url = urls[0]
    context.chat_data["awaiting_manual_emails"] = False
    context.user_data["awaiting_manual_email"] = False
    lock = context.chat_data.setdefault("extract_lock", asyncio.Lock())
    if lock.locked():
        await message.reply_text("⏳ Уже идёт анализ этого URL")
        return

    now = time.monotonic()
    last = context.chat_data.get("last_url")
    last_url = None
    last_ts = 0.0
    if isinstance(last, dict):
        if isinstance(last.get("urls"), list) and last.get("urls"):
            last_url = last["urls"][0]
        elif isinstance(last.get("url"), str):
            last_url = last["url"]
        try:
            last_ts = float(last.get("ts", 0) or 0)
        except Exception:
            last_ts = 0.0
    if last_url == url and now - last_ts < 10:
        await message.reply_text("⏳ Уже идёт анализ этого URL")
        return

    if not settings.ENABLE_WEB:
        await message.reply_text(
            "Веб-парсер отключён (ENABLE_WEB=0). Включи в .env и перезапусти бота."
        )
        return

    clear_stop()
    job_name = _build_parse_task_name(update, "url")
    current_task = asyncio.current_task()
    idle_seconds = _watchdog_idle_seconds()
    if current_task:
        register_task(job_name, current_task)
        asyncio.create_task(start_watchdog(current_task, idle_seconds=idle_seconds))

    status_msg = None
    pulse_task: asyncio.Task[None] | None = None

    async def _reply_status_error(text: str) -> None:
        if status_msg:
            try:
                await status_msg.edit_text(text)
            except Exception:
                pass
        else:
            try:
                await message.reply_text(text)
            except Exception:
                pass
    try:
        status_msg = await message.reply_text("⏳ Загружаю сайт, парсю адреса…")
        await heartbeat()
    except Exception:
        status_msg = None

    try:
        async with lock:
            context.chat_data["last_url"] = {"url": url, "urls": [url], "ts": now}
            context.chat_data["entry_url"] = url
            pulse_task = start_heartbeat_pulse(interval=5.0)
            from .digest import extract_from_url

            found = await extract_from_url(url, context=context)
            await heartbeat()
    except asyncio.CancelledError as exc:
        cancelled_text = (
            "⛔️ Задача прервана из-за отсутствия прогресса. Лог зависания сохранён в var/hang_dump.txt"
            if exc.args and exc.args[0] == "watchdog"
            else "🛑 Процесс был остановлен."
        )
        if status_msg:
            try:
                await status_msg.edit_text(cancelled_text)
            except Exception:
                pass
        else:
            try:
                await message.reply_text(cancelled_text)
            except Exception:
                pass
        raise
    except httpx.HTTPStatusError as exc:
        await _reply_status_error(
            "Сайт ответил статусом "
            f"{exc.response.status_code} при загрузке страницы.\nПопробуй позже или другую ссылку."
        )
        return
    except httpx.ConnectError:
        await _reply_status_error(
            "Не удалось подключиться к сайту. Проверь ссылку или доступность ресурса."
        )
        return
    except httpx.ReadTimeout:
        await _reply_status_error(
            "Таймаут чтения страницы. Попробуй ещё раз или укажи другую ссылку."
        )
        return
    except Exception as exc:  # pragma: no cover - defensive branch
        log_error(f"handle_url_text: {exc}")
        await _reply_status_error(f"Не удалось получить страницу: {type(exc).__name__}")
        return
    finally:
        if pulse_task:
            pulse_task.cancel()
            with suppress(asyncio.CancelledError):
                await pulse_task
        if current_task:
            unregister_task(job_name, current_task)

    if not found:
        if status_msg:
            try:
                await status_msg.edit_text("⛔️ Не удалось найти адреса")
            except Exception:
                pass
        explanation = (
            "😕 На присланной ссылке не удалось найти e-mail адреса.\n\n"
            "Что можно сделать:\n"
            "• На странице нет явных e-mail;\n"
            "• Контакты подгружаются скриптами (SPA/JS);\n"
            "• Сайт блокирует ботов/требует капчу;\n"
            "• Контакты спрятаны в PDF/изображениях.\n\n"
            "Попробуйте: сохранить страницу в PDF и прислать файл — парсер по файлам у нас уже работает."
        )
        await message.reply_text(explanation)
        return

    allowed_all: Set[str] = set()
    for raw_email in found:
        candidate = (normalize_email(raw_email) or raw_email or "").strip()
        if candidate:
            allowed_all.add(candidate)

    allowed_all, trunc_pairs = apply_numeric_truncation_removal(allowed_all)
    repairs = list(dict.fromkeys(trunc_pairs))

    technical_emails = [
        addr for addr in allowed_all if any(pattern in addr for pattern in TECH_PATTERNS)
    ]
    filtered = [addr for addr in allowed_all if addr not in technical_emails and is_allowed_tld(addr)]
    suspicious_numeric = sorted({addr for addr in filtered if is_numeric_localpart(addr)})
    foreign_raw = {addr for addr in allowed_all if not is_allowed_tld(addr)}

    state = get_state(context)
    state.all_emails.update(allowed_all)
    current = set(state.to_send)
    current.update(filtered)
    state.to_send = sorted(current)
    _register_sources(state, allowed_all, url)
    state.repairs = list(dict.fromkeys((state.repairs or []) + repairs))
    state.repairs_sample = sample_preview([f"{bad} → {good}" for (bad, good) in state.repairs], 6)

    foreign_total = set(state.foreign) | foreign_raw
    suspicious_total = sorted({addr for addr in state.to_send if is_numeric_localpart(addr)})
    blocked_after_parse = count_blocked(state.to_send)
    total_footnote = state.footnote_dupes

    context.user_data["last_parsed_emails"] = list(state.to_send)

    if status_msg:
        try:
            await status_msg.edit_text("✅ Готово. Формирую превью…")
        except Exception:
            pass
    await heartbeat()

    report = await _compose_report_and_save(
        context,
        state.all_emails,
        state.to_send,
        suspicious_total,
        sorted(foreign_total),
        total_footnote,
        blocked_after_parse=blocked_after_parse,
        raw_candidates=found,
    )
    await _send_combined_parse_response(message, context, report, state)
    await heartbeat()

    raise ApplicationHandlerStop


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process text messages for uploads, blocking or manual lists."""

    message = update.message
    if not message:
        return

    raw_text = message.text or ""
    text = raw_text
    user = update.effective_user
    uid = user.id if user else None
    if uid is not None:
        st = REPORT_STATE.get(uid)
        if isinstance(st, dict) and st.get("await") == "single_date":
            if not raw_text or not raw_text.strip():
                return
            try:
                start_dt, end_dt, ddmmyyyy = parse_user_date_once(raw_text)
            except Exception:
                await message.reply_text(
                    "Не удалось распознать дату. Пример: 29.10.2025 или 2025-10-29"
                )
                return
            base_dir_raw = st.get("base_dir")
            base_dir = (
                Path(base_dir_raw)
                if base_dir_raw
                else Path(os.getenv("REPORT_BASE_DIR", "var") or "var")
            )
            records = _load_audit_records(base_dir, start_dt, end_dt)
            summary = _summarize(records, start_dt, end_dt)
            REPORT_STATE.pop(uid, None)
            tzname = os.getenv("EMAILBOT_TZ", "Europe/Amsterdam")
            await message.reply_text(
                f"📅 Отчёт за {ddmmyyyy} ({tzname})\n\n{summary}",
                reply_markup=_report_menu_kb(),
            )
            return
        if isinstance(st, dict) and st.get("await") in {"date_or_range", "confirm"}:
            rng = _parse_date_range(raw_text)
            if not rng:
                await message.reply_text(
                    "Не удалось распознать дату.\n"
                    "Примеры:\n"
                    "• 29.10.2025 — один день\n"
                    "• 01.10.2025–15.10.2025 — диапазон\n"
                    "• 10.2025 или 2025-10 — месяц\n"
                    "• 2025 — год"
                )
                return
            start, end = rng
            base_dir_raw = st.get("base_dir")
            REPORT_STATE[uid] = {
                "await": "confirm",
                "start": start,
                "end": end,
                "base_dir": base_dir_raw,
            }
            label = _format_period_label(start, end)
            await message.reply_text(
                f"Период: {label}\nПодтвердить построение отчёта?",
                reply_markup=_confirm_period_kb(),
            )
            return
    has_url = _message_has_url(message, raw_text)

    if await _handle_bulk_edit_text(update, context, text):
        return
    if context.user_data.get("awaiting_block_email"):
        clean = _preclean_text_for_emails(text)
        clear_stop()
        raw_emails = await asyncio.to_thread(extract_emails_loose, clean)
        emails = {normalize_email(x) for x in raw_emails if "@" in x}
        added = [e for e in emails if add_blocked_email(e)]
        await update.message.reply_text(
            f"Добавлено в исключения: {len(added)}" if added else "Ничего не добавлено."
        )
        context.user_data["awaiting_block_email"] = False
        return
    if context.user_data.get("awaiting_manual_email"):
        if has_url:
            await message.reply_text(MANUAL_URL_REJECT_MESSAGE)
            return
        clear_stop()
        found = await asyncio.to_thread(extract_emails_manual, text)
        filtered = sorted(set(e.lower().strip() for e in found))
        logger.info(
            "Manual input parsing: raw=%r found=%r filtered=%r",
            text,
            found,
            filtered,
        )
        if context.user_data.get("text_corrections"):
            handled = await _apply_manual_text_corrections(update, context, text)
            if handled:
                return
        if filtered:
            allowed: list[str] = []
            dropped: list[tuple[str, str]] = []
            for item in filtered:
                normalized, drop_reason = _classify_manual_email(item)
                if drop_reason:
                    dropped.append((item, drop_reason))
                    continue
                if normalized:
                    allowed.append(normalized)

            stored = _update_manual_storage(context, allowed)
            context.chat_data["manual_drop_reasons"] = dropped
            context.user_data["awaiting_manual_email"] = False
            context.user_data.pop("state", None)
            context.user_data.pop("text_corrections", None)
            await _send_manual_summary(update, context, stored, dropped)
        else:
            await update.message.reply_text("❌ Не найдено ни одного email.")
        return
    if has_url:
        await handle_url_text(update, context)
        return
    await update.message.reply_text(
        "Пришлите файл (PDF/DOC/DOCX/XLS/XLSX/CSV/TXT/ZIP) или ссылку на сайт."
    )
    return


async def ask_include_numeric(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Ask whether numeric-only addresses should be added."""

    query = update.callback_query
    state = get_state(context)
    numeric = state.suspect_numeric
    if not numeric:
        await query.answer("Цифровых адресов нет", show_alert=True)
        return
    await query.answer()
    preview_list = numeric[:60]
    txt = (
        f"Найдено цифровых логинов: {len(numeric)}.\nБудут добавлены все.\n\nПример:\n"
        + "\n".join(preview_list)
    )
    more = len(numeric) - len(preview_list)
    if more > 0:
        txt += f"\n… и ещё {more}."
    await query.message.reply_text(
        txt,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Включить все цифровые",
                        callback_data="confirm_include_numeric",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "↩️ Отмена", callback_data="cancel_include_numeric"
                    )
                ],
            ]
        ),
    )


async def include_numeric_emails(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Include numeric-only e-mail addresses in the send list."""

    query = update.callback_query
    state = get_state(context)
    numeric = state.suspect_numeric
    if not numeric:
        await query.answer("Цифровых адресов нет", show_alert=True)
        return
    await query.answer()
    current = set(state.to_send)
    added = [e for e in numeric if e not in current]
    current.update(numeric)
    state.to_send = sorted(current)
    state.blocked_after_parse = count_blocked(state.to_send)
    await query.message.reply_text(
        (
            f"➕ Добавлено цифровых адресов: {len(added)}.\n"
            f"Итого к отправке: {len(state.to_send)}."
        )
    )


async def cancel_include_numeric(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Keep numeric addresses excluded from the send list."""

    query = update.callback_query
    await query.answer()
    await query.message.reply_text("Ок, цифровые адреса оставлены выключенными.")


async def show_numeric_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display a list of numeric-only e-mail addresses."""

    query = update.callback_query
    state = context.chat_data.get(SESSION_KEY)
    numeric = state.suspect_numeric if state else []
    if not numeric:
        await query.answer("Список пуст", show_alert=True)
        return
    await query.answer()
    for chunk in _chunk_list(numeric, 60):
        await query.message.reply_text("🔢 Цифровые логины:\n" + "\n".join(chunk))


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
        await query.message.reply_text("🌍 Иностранные домены:\n" + "\n".join(chunk))


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
    state.to_send = sorted(current)
    state.blocked_after_parse = count_blocked(state.to_send)
    txt = f"🧩 Применено исправлений: {applied}."
    if changed:
        txt += "\n" + "\n".join(changed)
        if applied > len(changed):
            txt += f"\n… и ещё {applied - len(changed)}."
    await query.message.reply_text(txt)


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
        await query.message.reply_text("🧩 Возможные исправления:\n" + "\n".join(chunk))


async def send_manual_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send e-mails entered manually by the user."""

    query = update.callback_query
    await query.answer()
    emails = context.user_data.get("manual_emails", [])
    if not emails:
        await query.message.reply_text("❗ Список email пуст.")
        return

    ignore_180d = bool(context.user_data.get("ignore_180d"))
    status_text = _cooldown_status(context)
    await query.message.reply_text(
        "Запущено — выполняю в фоне...\n"
        f"Правило 180 дней: {status_text}."
    )

    async def long_job() -> None:
        chat_id = query.message.chat.id
        # Безопасно извлекаем код группы (поддержка <3.9 и без падений на шумных коллбэках)
        group_code = (
            query.data[len("manual_group_") :]
            if (query.data or "").startswith("manual_group_")
            else (query.data or "")
        )
        template_path = TEMPLATE_MAP[group_code]

        start_cancel(chat_id)
        await query.message.reply_text(
            "⏳ Проверяю актуальность истории отправки…",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⏹️ Стоп", callback_data="stop_job")]]
            ),
        )
        did_sync = False
        with PerfTimer("imap_sync_gate_manual"):
            loop = asyncio.get_running_loop()
            did_sync, _, _ = await loop.run_in_executor(
                None, lambda: maybe_sync_before_send(logger=logger, chat_id=chat_id)
            )
            clear_recent_sent_cache()
        if did_sync:
            await query.message.reply_text("🔄 Обновила историю (6 мес) из IMAP (дельта).")
        else:
            await query.message.reply_text("✅ История свежая — синхронизация не требуется.")
        if is_cancelled(chat_id):
            clear_cancel(chat_id)
            await query.message.reply_text("⛔ Остановлено по запросу.")
            return

        # manual отправка не учитывает супресс-лист
        blocked = get_blocked_emails()
        sent_today = get_sent_today()
        lookup_days = int(os.getenv("EMAIL_LOOKBACK_DAYS", "180"))
        effective_lookup = 0 if ignore_180d else lookup_days

        try:
            host = os.getenv("IMAP_HOST", "imap.mail.ru")
            port = int(os.getenv("IMAP_PORT", "993"))
            imap = imaplib.IMAP4_SSL(host, port)
            imap.login(messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD)
            sent_folder = get_preferred_sent_folder(imap)
            imap.select(f'"{sent_folder}"')
        except Exception as e:
            log_error(f"imap connect: {e}")
            await query.message.reply_text(f"❌ IMAP ошибка: {e}")
            clear_cancel(chat_id)
            return

        with PerfTimer("filtering_manual"):
            to_send = build_send_list(
                emails,
                blocked,
                sent_today,
                lookup_days=effective_lookup,
            )

        if is_cancelled(chat_id):
            await query.message.reply_text("⛔ Остановлено (после фильтрации).")
            try:
                imap.logout()
            except Exception:
                pass
            clear_cancel(chat_id)
            return

        if not to_send:
            reason = "стоп-листе"
            if not ignore_180d:
                reason += ", истории за 6 месяцев"
            reason += " или уже отправлены сегодня"
            await query.message.reply_text(
                f"❗ Все адреса уже есть в {reason}."
            )
            context.user_data["manual_emails"] = []
            try:
                imap.logout()
            except Exception:
                pass
            clear_recent_sent_cache()
            clear_cancel(chat_id)
            return
        available = max(0, MAX_EMAILS_PER_DAY - len(sent_today))
        if available <= 0 and not is_force_send(chat_id):
            logger.info(
                "Daily limit reached: %s emails sent today (source=sent_log)",
                len(sent_today),
            )
            await update.callback_query.message.reply_text(
                (
                    f"❗ Дневной лимит {MAX_EMAILS_PER_DAY} уже исчерпан.\n"
                    "Если вы исправили ошибки — нажмите "
                    "«🚀 Игнорировать лимит» и запустите ещё раз."
                )
            )
            clear_cancel(chat_id)
            return
        if not is_force_send(chat_id) and len(to_send) > available:
            to_send = to_send[:available]
            await query.message.reply_text(
                (
                    f"⚠️ Учитываю дневной лимит: будет отправлено "
                    f"{available} адресов из списка."
                )
            )

        await query.message.reply_text(
            (
                f"✉️ Рассылка начата. Отправляем {len(to_send)} писем...\n"
                f"Правило 180 дней: {status_text}."
            )
        )

        sent_count = 0
        aborted = False
        error_details: list[str] = []
        duplicates: list[str] = []
        cancel_event = context.chat_data.get("cancel_event")
        host = os.getenv("SMTP_HOST", "smtp.mail.ru")
        port = int(os.getenv("SMTP_PORT", "465"))
        ssl_env = os.getenv("SMTP_SSL")
        use_ssl = None if not ssl_env else (ssl_env == "1")
        retries = int(os.getenv("SMTP_CONNECT_RETRIES", "3"))
        backoff = float(os.getenv("SMTP_CONNECT_BACKOFF", "1.0"))

        import smtplib  # наверху можно не поднимать, локальный импорт ок

        attempt = 0
        initial_count = len(to_send)
        with PerfTimer("sending_manual", extra={"count": initial_count}):
            while True:
                try:
                    with SmtpClient(
                        host,
                        port,
                        messaging.EMAIL_ADDRESS,
                        messaging.EMAIL_PASSWORD,
                        use_ssl=use_ssl,
                    ) as client:
                        def on_sent(
                            email_addr: str,
                            token: str,
                            log_key: str | None,
                            content_hash: str | None,
                        ) -> None:
                            try:
                                mark_soft_bounce_success(email_addr)
                            except Exception:
                                pass

                        def on_duplicate(email_addr: str) -> None:
                            duplicates.append(email_addr)
                            error_details.append("пропущено (дубль за 24 ч)")

                        def on_cooldown(email_addr: str) -> None:
                            error_details.append("пропущено (кулдаун 180 дней)")

                        def on_blocked(email_addr: str) -> None:
                            error_details.append("пропущено (стоп-лист)")

                        def on_unknown(email_addr: str) -> None:
                            error_details.append("ошибка отправки")

                        def on_error(
                            email_addr: str,
                            exc: Exception,
                            code: Optional[int],
                            msg: Optional[str],
                        ) -> None:
                            error_details.append(str(exc))
                            add_bounce(email_addr, code, str(msg or exc), phase="send")
                            if is_hard_bounce(code, msg):
                                suppress_add(email_addr, code, "hard bounce on send")
                            elif is_soft_bounce(code, msg):
                                try:
                                    code_int: Optional[int]
                                    try:
                                        code_int = int(code) if code is not None else None
                                    except Exception:
                                        code_int = None
                                    if isinstance(msg, (bytes, bytearray)):
                                        reason_text = msg.decode("utf-8", "ignore")
                                    else:
                                        reason_text = str(msg or exc)
                                    log_soft_bounce(
                                        email_addr,
                                        reason=reason_text,
                                        group_code=group_code,
                                        chat_id=chat_id,
                                        template_path=template_path,
                                        code=code_int,
                                    )
                                except Exception:
                                    pass

                        sent_now, aborted_now = await run_smtp_send(
                            client,
                            to_send,
                            template_path=template_path,
                            group_code=group_code,
                            imap=imap,
                            sent_folder=sent_folder,
                            chat_id=chat_id,
                            sleep_between=1.5,
                            cancel_event=cancel_event,
                            should_stop_cb=should_stop,
                            on_sent=on_sent,
                            on_duplicate=on_duplicate,
                            on_cooldown=on_cooldown,
                            on_blocked=on_blocked,
                            on_error=on_error,
                            on_unknown=on_unknown,
                            override_180d=ignore_180d,
                        )
                        sent_count += sent_now
                        aborted = aborted or aborted_now
                    break  # успешно отработали без коннект-ошибок
                except messaging.TemplateRenderError as err:
                    missing = ", ".join(sorted(err.missing)) if err.missing else "—"
                    await context.bot.send_message(
                        chat_id=query.message.chat.id,
                        text=(
                            "⚠️ Шаблон не готов к отправке.\n"
                            f"Файл: {err.path}\n"
                            f"Не заполнены: {missing}\n\n"
                            "Подставь значения или создай рядом файл с текстом письма:\n"
                            "• <имя_шаблона>.body.txt — будет вставлен в {BODY}/{BODY}."
                        ),
                    )
                    try:
                        imap.logout()
                    except Exception:
                        pass
                    clear_cancel(chat_id)
                    return
                except (smtplib.SMTPServerDisconnected, TimeoutError, OSError):
                    attempt += 1
                    if attempt >= retries:
                        clear_cancel(chat_id)
                        raise
                    await asyncio.sleep(backoff)
                    backoff *= 2
        clear_cancel(chat_id)
        imap.logout()
        if aborted:
            await query.message.reply_text(
                f"🛑 Остановлено. Отправлено писем: {sent_count}"
            )
        else:
            await query.message.reply_text(f"✅ Отправлено писем: {sent_count}")
        if error_details:
            summary = format_error_details(error_details)
            if summary:
                await query.message.reply_text(summary)
    
        context.user_data["manual_emails"] = []
        clear_recent_sent_cache()
        disable_force_send(chat_id)
    
        clear_stop()
        messaging.create_task_with_logging(
            long_job(),
            query.message.reply_text,
            task_name="manual_mass_send",
        )
    
    
    async def send_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Send all prepared e-mails respecting limits."""
    
        query = update.callback_query
        chat_id = query.message.chat.id
        saved = mass_state.load_chat_state(chat_id)
        if saved and saved.get("pending"):
            emails = saved.get("pending", [])
            group_code = saved.get("group")
            template_path = saved.get("template")
        else:
            state = get_state(context)
            emails = state.to_send
            group_code = state.group
            template_path = state.template
        if not emails or not group_code or not template_path:
            await query.answer("Нет данных для отправки", show_alert=True)
            return
        await query.answer()
        await query.message.reply_text("Запущено — выполняю в фоне...")

        async def long_job() -> None:
            start_cancel(chat_id)
            await query.message.reply_text(
                "⏳ Проверяю актуальность истории отправки…",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⏹️ Стоп", callback_data="stop_job")]]
                ),
            )
            state = get_state(context)
            with PerfTimer("imap_sync_gate_bulk"):
                loop = asyncio.get_running_loop()
                did_sync, _, _ = await loop.run_in_executor(
                    None, lambda: maybe_sync_before_send(logger=logger, chat_id=chat_id)
                )
                clear_recent_sent_cache()
            if did_sync:
                await query.message.reply_text("🔄 Обновила историю (6 мес) из IMAP (дельта).")
            else:
                await query.message.reply_text("✅ История свежая — синхронизация не требуется.")
            if is_cancelled(chat_id):
                clear_cancel(chat_id)
                await query.message.reply_text("⛔ Остановлено по запросу.")
                return

            with PerfTimer("filtering_bulk"):
                lookup_days = int(os.getenv("EMAIL_LOOKBACK_DAYS", "180"))
                blocked = get_blocked_emails()
                sent_today = get_sent_today()
        
                planned_total = len(emails)
                try:
                    planned_unique = len({normalize_email(e) for e in emails if e})
                except Exception:
                    planned_unique = len({(e or "").strip().lower() for e in emails if e})
        
                saved_state = mass_state.load_chat_state(chat_id)
                duplicates: List[str]
                batch_duplicates: List[str]
                recent_180d_examples: set[str] = set()
                today_examples: set[str] = set()
                invalid_examples: set[str] = set()
                foreign_examples: set[str] = set()
                dup_examples: set[str] = set()
                removed_today: list[str] = []
                ready_after_cooldown: list[str] = []
                digest: dict[str, object] = {}

                state.override_cooldown = bool(context.user_data.get("ignore_cooldown"))

                if saved_state and saved_state.get("pending"):
                    blocked_foreign = list(saved_state.get("blocked_foreign", []))
                    blocked_invalid = list(saved_state.get("blocked_invalid", []))
                    skipped_recent = list(saved_state.get("skipped_recent", []))
                    sent_ok = list(saved_state.get("sent_ok", []))
                    to_send = list(saved_state.get("pending", []))
                    duplicates = list(saved_state.get("skipped_duplicates", []))
                    batch_duplicates = []
                    dup_examples.update(duplicates)
                    recent_180d_examples.update(skipped_recent[:10])
                    foreign_examples.update(blocked_foreign[:10])
                    invalid_examples.update(blocked_invalid[:10])
                    snapshot = state.last_digest or _snapshot_mass_digest(
                        {
                            "ready_after_cooldown": len(to_send),
                            "removed_recent_180d": len(skipped_recent),
                            "removed_invalid": len(blocked_invalid),
                            "removed_foreign": len(blocked_foreign),
                            "removed_duplicates_in_batch": len(duplicates),
                            "set_planned": len(to_send),
                            "ready_final": len(to_send),
                        },
                        ready_after_cooldown=len(to_send),
                        ready_final=len(to_send),
                    )
                    state.last_digest = snapshot
                    context.chat_data["last_filter_digest"] = snapshot
                    logger.info(
                        "bulk_filter_digest",
                        extra={"digest": snapshot, "chat_id": chat_id},
                    )
                    digest = dict(snapshot)
                else:
                    blocked_foreign = []
                    blocked_invalid = []
                    skipped_recent = []
                    to_send = []
                    sent_ok = []
                    duplicates = []
                    batch_duplicates = []

                    filtered_initial: list[str] = []
                    for e in emails:
                        if e in blocked:
                            blocked_invalid.append(e)
                            invalid_examples.add(e)
                            continue
                        if e in sent_today:
                            removed_today.append(e)
                            today_examples.add(e)
                            continue
                        if is_foreign(e):
                            blocked_foreign.append(e)
                            foreign_examples.add(e)
                            continue
                        filtered_initial.append(e)

                    queue: List[str] = []
                    for e in filtered_initial:
                        if is_suppressed(e):
                            blocked_invalid.append(e)
                            invalid_examples.add(e)
                        else:
                            queue.append(e)

                    to_send = []
                    ignore_cooldown = bool(context.user_data.get("ignore_cooldown"))
                    state.override_cooldown = ignore_cooldown
                    for e in queue:
                        if not ignore_cooldown and was_emailed_recently(e, lookup_days):
                            skipped_recent.append(e)
                            recent_180d_examples.add(e)
                        else:
                            to_send.append(e)

                    ready_after_cooldown = list(to_send)

                    deduped: List[str] = []
                    seen_norm: Set[str] = set()
                    for e in to_send:
                        norm = normalize_email(e)
                        if norm in seen_norm:
                            batch_duplicates.append(e)
                            duplicates.append(e)
                            dup_examples.add(e)
                        else:
                            seen_norm.add(norm)
                            deduped.append(e)
                    to_send = deduped

                    digest = {
                        "input_total": len(emails),
                        "after_suppress": len(queue),
                        "foreign_blocked": len(blocked_foreign),
                        "ready_after_cooldown": len(ready_after_cooldown),
                        "removed_recent_180d": len(skipped_recent),
                        "removed_today": len(removed_today),
                        "removed_invalid": len(blocked_invalid),
                        "removed_foreign": len(blocked_foreign),
                        "removed_duplicates_in_batch": len(batch_duplicates),
                        "set_planned": len(to_send),
                        "ready_final": len(to_send),
                        "sent_planned": len(to_send),
                    }

                    snapshot = _snapshot_mass_digest(
                        digest,
                        ready_after_cooldown=len(ready_after_cooldown),
                        ready_final=len(to_send),
                    )
                    state.last_digest = snapshot
                    context.chat_data["last_filter_digest"] = snapshot

                    try:
                        var_dir = Path("var")
                        var_dir.mkdir(parents=True, exist_ok=True)
                        (var_dir / "last_batch_digest.json").write_text(
                            json.dumps(snapshot, ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )
                        examples = {
                            "recent_180d": sorted(recent_180d_examples)[:10],
                            "today": sorted(today_examples)[:10],
                            "invalid": sorted(invalid_examples)[:10],
                            "foreign": sorted(foreign_examples)[:10],
                            "duplicates_in_batch": sorted(dup_examples)[:10],
                        }
                        (var_dir / "last_batch_examples.json").write_text(
                            json.dumps(examples, ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )
                    except Exception:
                        logger.exception("failed_to_write_last_batch_digest")

                    logger.info(
                        "bulk_filter_digest",
                        extra={"digest": snapshot, "chat_id": chat_id},
                    )

                    log_mass_filter_digest({**digest, "chat_id": chat_id})

                    active_norms = set()
                    for addr in to_send + duplicates:
                        norm = normalize_email(addr) or str(addr).strip().lower()
                        if norm:
                            active_norms.add(norm)
                    filtered_sources = {}
                    if getattr(state, "source_map", None):
                        try:
                            filtered_sources = {
                                norm: list(state.source_map.get(norm, []))
                                for norm in active_norms
                                if norm in state.source_map
                            }
                        except Exception:
                            filtered_sources = {}

                    mass_state.save_chat_state(
                        chat_id,
                        {
                            "group": group_code,
                            "template": template_path,
                            "pending": to_send,
                            "sent_ok": sent_ok,
                            "blocked_foreign": blocked_foreign,
                            "blocked_invalid": blocked_invalid,
                            "skipped_recent": skipped_recent,
                            "skipped_duplicates": duplicates,
                            "source_map": filtered_sources,
                        },
                    )

                digest_snapshot = state.last_digest or _snapshot_mass_digest(
                    digest,
                    ready_after_cooldown=len(to_send),
                    ready_final=len(to_send),
                )

            if is_cancelled(chat_id):
                clear_cancel(chat_id)
                await query.message.reply_text("⛔ Остановлено (после фильтрации).")
                return

            limited_from: int | None = None

            if not to_send:
                explanation = _format_empty_send_explanation(digest_snapshot)
                await query.message.reply_text(
                    explanation,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                clear_cancel(chat_id)
                return

        available = max(0, MAX_EMAILS_PER_DAY - len(sent_today))
        if available <= 0 and not is_force_send(chat_id):
            logger.info(
                "Daily limit reached: %s emails sent today (source=sent_log)",
                len(sent_today),
            )
            await query.message.reply_text(
                (
                    f"❗ Дневной лимит {MAX_EMAILS_PER_DAY} уже исчерпан.\n"
                    "Если вы исправили ошибки — нажмите "
                    "«🚀 Игнорировать лимит» и запустите ещё раз."
                )
            )
            clear_cancel(chat_id)
            return
        if not is_force_send(chat_id) and len(to_send) > available:
            limited_from = len(to_send)
            to_send = to_send[:available]
            await query.message.reply_text(
                (
                    f"⚠️ Учитываю дневной лимит: будет отправлено "
                    f"{available} адресов из списка."
                )
            )
            limited_norms = set()
            for addr in to_send + duplicates:
                norm = normalize_email(addr) or str(addr).strip().lower()
                if norm:
                    limited_norms.add(norm)
            limited_sources = {}
            if getattr(state, "source_map", None):
                try:
                    limited_sources = {
                        norm: list(state.source_map.get(norm, []))
                        for norm in limited_norms
                        if norm in state.source_map
                    }
                except Exception:
                    limited_sources = {}
            mass_state.save_chat_state(
                chat_id,
                {
                    "group": group_code,
                    "template": template_path,
                    "pending": to_send,
                    "sent_ok": sent_ok,
                    "blocked_foreign": blocked_foreign,
                    "blocked_invalid": blocked_invalid,
                    "skipped_recent": skipped_recent,
                    "skipped_duplicates": duplicates,
                    "source_map": limited_sources,
                },
            )

        start_text = format_dispatch_start(
            planned_total,
            planned_unique,
            len(to_send),
            deferred=len(skipped_recent),
            suppressed=len(blocked_invalid),
            foreign=len(blocked_foreign),
            duplicates=len(batch_duplicates),
            limited_from=limited_from,
        )
        await notify(query.message, start_text, event="start")

        try:
            host = os.getenv("IMAP_HOST", "imap.mail.ru")
            port = int(os.getenv("IMAP_PORT", "993"))
            imap = imaplib.IMAP4_SSL(host, port)
            imap.login(messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD)
            sent_folder = get_preferred_sent_folder(imap)
            imap.select(f'"{sent_folder}"')
        except Exception as e:
            log_error(f"imap connect: {e}")
            await query.message.reply_text(f"❌ IMAP ошибка: {e}")
            clear_cancel(chat_id)
            return

        error_details: list[str] = []
        cancel_event = context.chat_data.get("cancel_event")
        aborted = False
        initial_count = len(to_send)
        with PerfTimer("sending_bulk", extra={"count": initial_count}):
            with SmtpClient(
                "smtp.mail.ru", 465, messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD
            ) as client:
                def on_sent(
                    email_addr: str,
                    token: str,
                    log_key: str | None,
                    content_hash: str | None,
                ) -> None:
                    sent_ok.append(email_addr)
                    try:
                        mark_soft_bounce_success(email_addr)
                    except Exception:
                        pass

                def on_duplicate(email_addr: str) -> None:
                    duplicates.append(email_addr)
                    error_details.append("пропущено (дубль за 24 ч)")

                def on_cooldown(email_addr: str) -> None:
                    error_details.append("пропущено (кулдаун 180 дней)")
                    if email_addr not in skipped_recent:
                        skipped_recent.append(email_addr)

                def on_blocked(email_addr: str) -> None:
                    error_details.append("пропущено (стоп-лист)")
                    if email_addr not in blocked_invalid:
                        blocked_invalid.append(email_addr)

                def on_unknown(email_addr: str) -> None:
                    error_details.append("ошибка отправки")

                def on_error(
                    email_addr: str,
                    exc: Exception,
                    code: Optional[int],
                    msg: Optional[str],
                ) -> None:
                    error_details.append(str(exc))
                    add_bounce(email_addr, code, str(msg or exc), phase="send")
                    if is_hard_bounce(code, msg):
                        suppress_add(email_addr, code, "hard bounce on send")
                    elif is_soft_bounce(code, msg):
                        try:
                            code_int: Optional[int]
                            try:
                                code_int = int(code) if code is not None else None
                            except Exception:
                                code_int = None
                            if isinstance(msg, (bytes, bytearray)):
                                reason_text = msg.decode("utf-8", "ignore")
                            else:
                                reason_text = str(msg or exc)
                            log_soft_bounce(
                                email_addr,
                                reason=reason_text,
                                group_code=group_code,
                                chat_id=chat_id,
                                template_path=template_path,
                                code=code_int,
                            )
                        except Exception:
                            pass

                def after_each(email_addr: str) -> None:
                    current_norms = set()
                    for addr in to_send + duplicates:
                        norm = normalize_email(addr) or str(addr).strip().lower()
                        if norm:
                            current_norms.add(norm)
                    current_sources = {}
                    if getattr(state, "source_map", None):
                        try:
                            current_sources = {
                                norm: list(state.source_map.get(norm, []))
                                for norm in current_norms
                                if norm in state.source_map
                            }
                        except Exception:
                            current_sources = {}
                    mass_state.save_chat_state(
                        chat_id,
                        {
                            "group": group_code,
                            "template": template_path,
                            "pending": to_send,
                            "sent_ok": sent_ok,
                            "blocked_foreign": blocked_foreign,
                            "blocked_invalid": blocked_invalid,
                            "skipped_recent": skipped_recent,
                            "skipped_duplicates": duplicates,
                            "source_map": current_sources,
                        },
                    )

                try:
                    sent_now, aborted_now = await run_smtp_send(
                        client,
                        to_send,
                        template_path=template_path,
                        group_code=group_code,
                        imap=imap,
                        sent_folder=sent_folder,
                        chat_id=chat_id,
                        sleep_between=1.5,
                        cancel_event=cancel_event,
                        should_stop_cb=should_stop,
                        on_sent=on_sent,
                        on_duplicate=on_duplicate,
                        on_cooldown=on_cooldown,
                        on_blocked=on_blocked,
                        on_error=on_error,
                        on_unknown=on_unknown,
                        after_each=after_each,
                    )
                except messaging.TemplateRenderError as err:
                    missing = ", ".join(sorted(err.missing)) if err.missing else "—"
                    await context.bot.send_message(
                        chat_id=query.message.chat.id,
                        text=(
                            "⚠️ Шаблон не готов к отправке.\n"
                            f"Файл: {err.path}\n"
                            f"Не заполнены: {missing}\n\n"
                            "Подставь значения или создай рядом файл с текстом письма:\n"
                            "• <имя_шаблона>.body.txt — будет вставлен в {BODY}/{BODY}."
                        ),
                    )
                    try:
                        imap.logout()
                    except Exception:
                        pass
                    clear_cancel(chat_id)
                    return
                aborted = aborted or aborted_now
        clear_cancel(chat_id)
        if not to_send:
            mass_state.clear_chat_state(chat_id)

        total_planned = initial_count
        try:
            stoplist_blocked = count_blocked(blocked_invalid)
        except Exception:
            stoplist_blocked = 0
        undeliverable_only = max(0, len(blocked_invalid) - stoplist_blocked)
        fallback_total = total_planned or (
            len(sent_ok)
            + len(skipped_recent)
            + stoplist_blocked
            + undeliverable_only
            + len(duplicates)
        )
        fallback_metrics = {
            "total": fallback_total,
            "sent": len(sent_ok),
            "blocked": stoplist_blocked,
            "cooldown": len(skipped_recent),
            "undeliverable_only": undeliverable_only,
            "unchanged": len(duplicates),
            "errors": len(error_details),
        }
        audit_path = None
        try:
            audit_path = context.chat_data.get("bulk_audit_path")
        except Exception:
            audit_path = None
        metrics = fallback_metrics
        if audit_path:
            metrics = _summarize_from_audit(str(audit_path))
            if not metrics.get("total") and fallback_metrics["total"]:
                metrics = fallback_metrics

        summary_lines: list[str] = []
        summary_lines.append("📨 Рассылка завершена.")
        summary_lines.append(f"📊 В очереди было: {metrics['total']}")
        summary_lines.append(f"✅ Отправлено: {metrics['sent']}")
        summary_lines.append(
            f"⏳ Пропущены (по правилу «180 дней»): {metrics['cooldown']}"
        )
        summary_lines.append(f"🚫 В стоп-листе: {metrics['blocked']}")
        summary_lines.append(f"ℹ️ Осталось без изменений: {metrics['unchanged']}")
        summary_lines.append(
            f"🚫 Недоставляемые (без стоп-листа): {metrics['undeliverable_only']}"
        )
        summary_lines.append(f"❌ Ошибок при отправке: {metrics['errors']}")
        if aborted:
            summary_lines.append("⛔ Рассылка остановлена досрочно.")
        if blocked_foreign:
            summary_lines.append(
                f"🌍 Иностранные домены (отложены): {len(blocked_foreign)}"
            )
        if audit_path:
            summary_lines.append(f"📄 Аудит: {audit_path}")

        await query.message.reply_text("\n".join(summary_lines))
        if error_details:
            summary = format_error_details(error_details)
            if summary:
                await query.message.reply_text(summary)

        clear_recent_sent_cache()
        disable_force_send(chat_id)

    clear_stop()
    messaging.create_task_with_logging(
        long_job(),
        query.message.reply_text,
        task_name="bulk_mass_send",
    )


async def show_skipped_examples(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Display sample e-mail addresses filtered out for a specific reason."""

    query = update.callback_query
    data = query.data or ""
    reason = data.split(":", 1)[1] if ":" in data else ""
    message = query.message
    chat_id = message.chat.id if message else None

    if message is None or chat_id is None or not reason:
        await query.answer("Некорректный запрос", show_alert=True)
        return

    summary = load_last_summary(chat_id)
    if not summary:
        await query.answer("Нет сохранённой сводки для этого чата.", show_alert=True)
        return

    skipped_raw = summary.get("skipped") if isinstance(summary, dict) else None
    entries = []
    if isinstance(skipped_raw, dict):
        raw = skipped_raw.get(reason) or []
        if isinstance(raw, list):
            entries = _unique_preserve_order(str(item) for item in raw)

    if not entries:
        await query.answer("Нет адресов по этой причине.", show_alert=True)
        return

    total = len(entries)
    sample = entries[:SKIPPED_PREVIEW_LIMIT]
    label = _SKIPPED_REASON_LABELS.get(reason, reason)
    lines = [f"Причина: {label}"]
    lines.append(f"Показано {len(sample)} из {total}:")
    lines.extend(sample)
    await message.reply_text("\n".join(lines))
    await query.answer()


async def autosync_imap_with_message(query: CallbackQuery) -> None:
    """Synchronize IMAP logs and notify the user via message."""
    await query.answer()
    await query.message.reply_text("🔄 Синхронизация истории отправки с сервером...")
    loop = asyncio.get_running_loop()
    stats = await loop.run_in_executor(None, sync_log_with_imap)
    clear_recent_sent_cache()
    await query.message.reply_text(
        "✅ Синхронизация завершена. "
        f"новых: {stats['new_contacts']}, обновлено: {stats['updated_contacts']}, "
        f"пропущено: {stats['skipped_events']}, всего: {stats['total_rows_after']}.\n"
        f"История отправки обновлена на последние 6 месяцев."
    )


async def stop_job_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the ⏹️ stop button by requesting cancellation for the chat."""

    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    request_cancel(chat_id)
    await query.message.reply_text(
        "🛑 Запрос на остановку принят. Завершаю текущую операцию…"
    )


async def start_sending_quick(
    update: Update, context: ContextTypes.DEFAULT_TYPE, group: str
) -> None:
    """Упрощённый запуск массовой рассылки из последнего предпросмотра."""

    emails = list(context.user_data.get("last_ready_emails") or [])
    if not emails:
        query = getattr(update, "callback_query", None)
        if query is not None:
            try:
                await query.answer()
            except Exception:  # pragma: no cover - best effort acknowledgement
                pass
        await update.effective_chat.send_message(
            "Список пуст — сначала загрузите данные."
        )
        return

    await update.effective_chat.send_message(
        f"✉️ Рассылка начата. Отправляем {len(emails)} писем..."
    )

    from .handlers.manual_send import queue_and_send

    await queue_and_send(update, context, template_key=group)


# --- Совместимость: обёртки под старые имена хендлеров ---


async def start_sending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запуск массовой рассылки по подготовленному ``batch_id``."""

    try:
        manual = context.user_data.get("manual_emails")
    except Exception:
        manual = None

    if manual:
        return await send_manual_email(update, context)

    query = getattr(update, "callback_query", None)
    if query is not None:
        try:
            await query.answer()
        except Exception:  # pragma: no cover - defensive branch
            pass

        cbid = getattr(query, "id", None)
        if cbid:
            cbmap_raw = context.bot_data.setdefault("seen_callback_ids", {})
            try:
                cbmap = dict(cbmap_raw)
            except Exception:
                cbmap = {}
            now_m = time.monotonic()
            for old_id, ts in list(cbmap.items()):
                try:
                    ts_float = float(ts)
                except Exception:
                    cbmap.pop(old_id, None)
                    continue
                if now_m - ts_float > 90.0:
                    cbmap.pop(old_id, None)
            if cbmap.get(cbid):
                return
            cbmap[cbid] = now_m
            context.bot_data["seen_callback_ids"] = cbmap

    try:
        bulk_handler = context.chat_data.get("bulk_handler")
    except Exception:
        bulk_handler = None

    data = query.data if query is not None else None
    batch_id = ""
    if data:
        if ":" in data:
            batch_id = data.split(":", 1)[1]
        else:
            batch_id = data

    if not batch_id and isinstance(bulk_handler, dict):
        raw_batch = bulk_handler.get("batch_id")
        if isinstance(raw_batch, str) and raw_batch:
            batch_id = raw_batch

    if not batch_id:
        message = "Не найден batch_id. Нажмите «Показать примеры» заново."
        if query is not None:
            try:
                await _safe_edit_message(query, text=message)
                return
            except Exception:
                pass
        message_obj = update.effective_message
        if message_obj is not None:
            try:
                await message_obj.reply_text(message)
            except Exception:
                pass
        return

    batches = context.bot_data.get("bulk_batches") or {}
    queue = batches.get(batch_id)
    if not isinstance(queue, dict):
        logger.warning("start_sending: batch not found: %s", batch_id)
        message = (
            "Не удалось найти подготовленный список (batch). "
            "Нажмите «Показать примеры» заново."
        )
        if query is not None:
            try:
                await _safe_edit_message(query, text=message)
                return
            except Exception:
                pass
        message_obj = update.effective_message
        if message_obj is not None:
            try:
                await message_obj.reply_text(message)
            except Exception:
                pass
        return

    emails_in_queue = queue.get("emails")
    if not emails_in_queue:
        logger.warning("start_sending: empty batch=%s", batch_id)
        empty_message = (
            "Очередь пуста. Нажмите «Показать примеры» или выберите направление заново."
        )
        if query is not None:
            try:
                await _safe_edit_message(query, text=empty_message)
                return
            except Exception:
                pass
        message_obj = update.effective_message
        if message_obj is not None:
            try:
                await message_obj.reply_text(empty_message)
            except Exception:
                pass
        return

    try:
        queue_size = len(emails_in_queue)
    except Exception:
        queue_size = 0

    group_code = queue.get("group")
    logger.info(
        "start_sending: using batch=%s group=%s emails=%d",
        batch_id,
        group_code,
        queue_size,
    )

    smap = context.bot_data.setdefault("bulk_status_by_batch", {})
    dmap = context.bot_data.setdefault("bulk_debounce_by_batch", {})
    lmap = context.bot_data.setdefault("bulk_locks_by_batch", {})

    now = time.monotonic()
    try:
        last_start = float(dmap.get(batch_id, 0.0))
    except Exception:
        last_start = 0.0
    current_status = smap.get(batch_id)
    if (now - last_start) < 8.0 or current_status in {"starting", "running"}:
        logger.info(
            "start_sending: debounce — status=%s, batch=%s (last=%.2fs)",
            current_status,
            batch_id,
            now - last_start,
        )
        warning_markup = _build_stop_markup()
        warning_text = (
            "Рассылка уже запускается/идёт. Пожалуйста, не нажимайте кнопку повторно."
        )
        if query is not None:
            try:
                await _safe_edit_message(
                    query,
                    text=warning_text,
                    reply_markup=warning_markup,
                )
                return
            except Exception:
                pass
        message_obj = update.effective_message
        if message_obj is not None:
            try:
                await message_obj.reply_text(warning_text, reply_markup=warning_markup)
            except Exception:
                pass
        return

    dmap[batch_id] = now
    context.bot_data["bulk_debounce_by_batch"] = dmap

    smap[batch_id] = "starting"
    context.bot_data["bulk_status_by_batch"] = smap

    start_markup = _build_stop_markup()
    start_text = (
        "🚀 Запускаю рассылку… Это может занять время. Вы можете смотреть прогресс в этом чате."
    )

    if query is not None:
        try:
            await _safe_edit_message(query, text=start_text, reply_markup=start_markup)
        except Exception:
            message_obj = update.effective_message
            if message_obj is not None:
                try:
                    await message_obj.reply_text(start_text, reply_markup=start_markup)
                except Exception:
                    pass
    else:
        message_obj = update.effective_message
        if message_obj is not None:
            try:
                await message_obj.reply_text(start_text, reply_markup=start_markup)
            except Exception:
                pass

    lock = lmap.get(batch_id)
    if lock is None:
        lock = asyncio.Lock()
        lmap[batch_id] = lock
        context.bot_data["bulk_locks_by_batch"] = lmap

    async def _run_bulk_send(
        handler_payload: dict[str, object],
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        handler = _resolve_mass_handler()
        if not callable(handler):
            # [EBOT-073] Показываем подробную причину, собранную при импорте
            err_hint = _LEGACY_MASS_SENDER_ERR or "handler unresolved"
            logger.error(
                "start_sending: no bulk handler available (reason: %s)", err_hint
            )
            smap[batch_id] = "error"
            context.bot_data["bulk_status_by_batch"] = smap
            chat_id_local = handler_payload.get("chat_id")
            if chat_id_local is not None:
                try:
                    await context.bot.send_message(
                        chat_id=chat_id_local,
                        text=(
                            "🚫 Не удалось запустить рассылку: не найден обработчик массовой отправки.\n"
                            f"Причина: {err_hint}\n"
                            "Если вы только что обновили код — перезапустите бота. "
                            "Также убедитесь, что модуль emailbot.handlers.manual_send доступен."
                        ),
                    )
                except Exception:  # pragma: no cover - best-effort уведомление
                    pass
            return

        app = getattr(context, "application", None)
        chat_id_local = handler_payload.get("chat_id")
        callback_map: dict[str, Callable[[str], None]] = {}
        if app is not None and chat_id_local is not None:
            def _send_with_prefix(prefix: str, text: str) -> None:
                if not text:
                    return
                try:
                    messaging.run_in_app_loop(
                        app,
                        messaging.reply(
                            chat_id_local,
                            f"{prefix}{text}" if prefix else text,
                        ),
                    )
                except Exception:
                    pass

            def _wrap(prefix: str) -> Callable[[str], None]:
                def _cb(text: str) -> None:
                    try:
                        _send_with_prefix(prefix, text)
                    except Exception:
                        pass

                return _cb

            callback_map = {
                "on_info": _wrap("ℹ️ "),
                "on_progress": _wrap(""),
                "on_error": _wrap("❌ "),
            }

        async with lock:
            smap[batch_id] = "running"
            context.bot_data["bulk_status_by_batch"] = smap
            context.chat_data["bulk_handler"] = handler_payload

            try:
                signature: inspect.Signature | None
                try:
                    signature = inspect.signature(handler)
                except (TypeError, ValueError):
                    signature = None

                accepts_kwargs = False
                positional_names: list[str] = []
                if signature is not None:
                    params = list(signature.parameters.values())
                    accepts_kwargs = any(
                        param.kind == inspect.Parameter.VAR_KEYWORD for param in params
                    )
                    positional_names = [
                        param.name
                        for param in params
                        if param.kind
                        in (
                            inspect.Parameter.POSITIONAL_ONLY,
                            inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        )
                    ]

                handler_kwargs: dict[str, Callable[[str], None]] = {}
                if callback_map:
                    for name, cb in callback_map.items():
                        if signature is None or accepts_kwargs or (
                            signature and name in signature.parameters
                        ):
                            handler_kwargs[name] = cb

                call_args: tuple[object, ...] = (update, context)
                if positional_names:
                    first_name = positional_names[0]
                    if first_name in {"context", "ctx"}:
                        call_args = (context,)
                    elif tuple(positional_names[:2]) in (
                        ("update", "context"),
                        ("update", "ctx"),
                    ):
                        call_args = (update, context)
                    elif positional_names == ["update"]:
                        call_args = (update,)

                async def _invoke(args: tuple[object, ...]) -> None:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(*args, **handler_kwargs)
                        return
                    result_obj = await asyncio.to_thread(
                        handler, *args, **handler_kwargs
                    )
                    if inspect.isawaitable(result_obj):
                        await result_obj

                await _invoke(call_args)
            except Exception as exc:
                logger.exception("start_sending: background bulk error: %s", exc)
                smap[batch_id] = "error"
                context.bot_data["bulk_status_by_batch"] = smap
                try:
                    chat_id_local = handler_payload.get("chat_id")
                    if chat_id_local is not None:
                        await context.bot.send_message(
                            chat_id=chat_id_local,
                            text=(
                                "❌ Ошибка при запуске/выполнении рассылки. "
                                "Откройте «Диагностика» и пришлите лог."
                            ),
                        )
                except Exception:  # pragma: no cover - best-effort notification
                    pass
                return

            batches_map = context.bot_data.setdefault("bulk_batches", {})
            batches_map.pop(batch_id, None)
            context.bot_data["bulk_batches"] = batches_map

            smap[batch_id] = "done"
            context.bot_data["bulk_status_by_batch"] = smap

            stored_handler = context.chat_data.get("bulk_handler")
            if isinstance(stored_handler, dict) and stored_handler.get("batch_id") == batch_id:
                context.chat_data.pop("bulk_handler", None)

            logger.info(
                "start_sending: finished, batch=%s, group=%s",
                batch_id,
                handler_payload.get("group"),
            )

    async def _runner() -> None:
        handler_queue: dict[str, object] = {
            "emails": list(queue.get("emails") or []),
            "group": queue.get("group"),
            "chat_id": queue.get("chat_id"),
            "digest": dict(queue.get("digest") or {}),
            "batch_id": batch_id,
        }
        template_value = queue.get("template")
        if isinstance(template_value, str) and template_value:
            handler_queue["template"] = template_value

        bh_copy = dict(handler_queue)

        await _run_bulk_send(bh_copy, update, context)

    app_for_tasks = getattr(context, "application", None)
    if app_for_tasks is not None:
        app_for_tasks.create_task(_runner())
    else:
        asyncio.create_task(_runner())


async def send_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Совместимая обёртка для старого имени массовой отправки."""

    mass_handler = globals().get("send_selected")
    if mass_handler is send_all:
        mass_handler = None
    if mass_handler is None:
        mass_handler = _resolve_mass_handler()

    if mass_handler is None or mass_handler is send_all:
        return await start_sending(update, context)

    return await mass_handler(update, context)


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
    "url_command",
    "crawl_command",
    "report_callback",
    "report_period",
    "on_diagnostics",
    "sync_imap_command",
    "reset_email_list",
    "diag",
    "selfcheck_command",
    "dedupe_log_command",
    "handle_document",
    "handle_drop",
    "refresh_preview",
    "proceed_to_group",
    "open_dirs_callback",
    "toggle_ignore_180",
    "select_group",
    "prompt_manual_email",
    "manual_input_router",
    "manual_start",
    "manual_select_group",
    "enable_text_corrections",
    "bulk_txt_start",
    "bulk_delete_start",
    "bulk_delete_text",
    "toggle_ignore_180d",
    "route_text_message",
    "handle_text",
    "ask_include_numeric",
    "include_numeric_emails",
    "cancel_include_numeric",
    "show_numeric_list",
    "show_foreign_list",
    "apply_repairs",
    "show_repairs",
    "start_sending",  # совместимость для старых точек входа
    "start_sending_quick",
    "send_manual_email",
    "send_all",
    "autosync_imap_with_message",
    "stop_job_callback",
    "show_skipped_menu",
    "show_skipped_examples",
]
