from __future__ import annotations

import io
import logging
import os
import re
import sys
from typing import Dict, List

import requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.error import Conflict, NetworkError, TelegramError
from telegram.ext import CommandHandler, Filters, MessageHandler, Updater
from telegram.utils.request import Request

from emailbot.messaging_utils import is_blocked, is_suppressed
from emailbot.pipelines.ingest import ingest_emails
from emailbot.utils.email_clean import clean_and_normalize_email
from emailbot.utils.file_email_extractor import ExtractError, extract_emails_from_bytes
from emailbot.utils.single_instance import single_instance_lock

LOGLEVEL = os.getenv("LOGLEVEL", "INFO")
logging.basicConfig(level=LOGLEVEL)
logger = logging.getLogger(__name__)

URL_RE = re.compile(r"(https?://[^\s]+)", re.IGNORECASE)
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}")


def start(update, context):
    update.message.reply_text("📥 Загрузите данные с e-mail-адресами или пришлите ссылку.")


def _render_stats(
    ok: List[str],
    rejects: Dict[str, int],
    warn: str | None = None,
    errors: List[str] | None = None,
    *,
    blocked_hits: int = 0,
    total_found: int | None = None,
) -> str:
    found = total_found if total_found is not None else len(ok) + blocked_hits
    txt = f"Найдено адресов: {found}"
    txt += f"\n📦 К отправке: {len(ok)}"
    if blocked_hits:
        txt += f"\n🚫 В стоп-листе: {blocked_hits}"
    if rejects:
        txt += "\nПричины отбраковки:" + "".join(f"\n • {key} — {val}" for key, val in rejects.items())
    if warn:
        txt += f"\n\n⚠️ {warn}"
    if errors:
        txt += "\n\nНе удалось обработать:\n" + "\n".join(f" • {err}" for err in errors)
    if ok:
        txt += "\n\nПримеры:\n" + "\n".join(f"`{addr}`" for addr in ok[:5])
    return txt


def ingest(update, context):
    lines = [line for line in update.message.text.splitlines()[1:] if line.strip()]
    ok, _bad, stats = ingest_emails(lines)
    txt = (
        f"Всего строк: {stats['total_in']}\n"
        f"Годных адресов: {stats['ok']}\n"
        f"Отброшено: {stats['bad']}"
    )
    rejects = stats.get("rejects")
    if rejects:
        txt += "\nПричины отбраковки:" + "".join(
            f"\n • {key} — {val}" for key, val in rejects.items()
        )
    if ok:
        txt += "\n\nПримеры:\n" + "\n".join(f"`{addr}`" for addr in ok[:5])
    update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)


def handle_url(update, context):
    text = update.message.text or ""
    urls = URL_RE.findall(text)
    if not urls:
        return
    ack = update.message.reply_text("Приняла ссылку, парсю страницу…")
    total_ok: List[str] = []
    total_rejects: Dict[str, int] = {}
    errors: List[str] = []
    found_addresses: set[str] = set()
    blocked_addresses: set[str] = set()

    for url in urls:
        try:
            resp = requests.get(url, timeout=15, headers={"User-Agent": "EmailBot/ptb"})
            if resp.status_code != 200:
                errors.append(f"{url} — http_status_{resp.status_code}")
                continue
            html = resp.text
            candidates = EMAIL_RE.findall(html)
            for raw in candidates:
                email, reason = clean_and_normalize_email(raw)
                if email is None:
                    key = str(reason) if reason else "unknown"
                    total_rejects[key] = total_rejects.get(key, 0) + 1
                    continue
                found_addresses.add(email)
                blocked = is_blocked(email)
                suppressed = is_suppressed(email)
                if blocked:
                    blocked_addresses.add(email)
                if blocked or suppressed:
                    continue
                total_ok.append(email)
        except Exception as exc:  # pragma: no cover - network errors
            errors.append(f"{url} — {type(exc).__name__}")

    total_ok = list(dict.fromkeys(total_ok))
    total_found = len(found_addresses)
    blocked_hits = len(blocked_addresses)
    ack.edit_text(
        _render_stats(
            total_ok,
            total_rejects,
            errors=errors,
            blocked_hits=blocked_hits,
            total_found=total_found,
        ),
        parse_mode=ParseMode.MARKDOWN,
    )


def handle_document(update, context):
    doc = update.message.document
    if not doc:
        return
    ack = update.message.reply_text(f"Приняла файл: {doc.file_name}. Обрабатываю…")
    file_obj = context.bot.get_file(doc.file_id)
    buf = io.BytesIO()
    file_obj.download(out=buf)
    data = buf.getvalue()
    try:
        ok, rejects, warn = extract_emails_from_bytes(data, doc.file_name or "file")
        found_addresses: set[str] = set()
        blocked_addresses: set[str] = set()
        filtered: List[str] = []
        for addr in ok:
            found_addresses.add(addr)
            blocked = is_blocked(addr)
            suppressed = is_suppressed(addr)
            if blocked:
                blocked_addresses.add(addr)
            if blocked or suppressed:
                continue
            filtered.append(addr)
        ok = list(dict.fromkeys(filtered))
        total_found = len(found_addresses)
        blocked_hits = len(blocked_addresses)
        ack.edit_text(
            _render_stats(
                ok,
                rejects,
                warn=warn,
                blocked_hits=blocked_hits,
                total_found=total_found,
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
    except ExtractError as exc:
        ack.edit_text(f"Не удалось обработать файл: {exc}")
    except Exception:  # pragma: no cover - defensive
        ack.edit_text("Произошла ошибка при разборе файла.")


def build_mass_preview_keyboard(batch_id: str) -> InlineKeyboardMarkup:
    """Клавиатура предпросмотра рассылки с кнопкой запуска."""

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🚀 Начать рассылку",
                    callback_data=f"bulk_start:{batch_id}",
                )
            ],
        ]
    )


def _parse_timeout(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
        if value <= 0:
            raise ValueError
        return value
    except ValueError:
        logger.warning("Invalid %s=%r. Using default %s s.", name, raw, default)
        return default


def main():
    if os.getenv("TELEGRAM_LEGACY_UI_ENABLED", "0") != "1":
        print(
            "Legacy telegram_ui disabled. Set TELEGRAM_LEGACY_UI_ENABLED=1 to enable.",
            file=sys.stderr,
        )
        return

    connect_timeout = _parse_timeout("TELEGRAM_CONNECT_TIMEOUT", 10)
    read_timeout = _parse_timeout("TELEGRAM_READ_TIMEOUT", 30)
    request = Request(connect_timeout=connect_timeout, read_timeout=read_timeout)
    updater = Updater(
        token=os.environ["TELEGRAM_BOT_TOKEN"], request=request, use_context=True
    )
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("ingest", ingest))
    dp.add_handler(MessageHandler(Filters.text & (~Filters.command), handle_url))
    dp.add_handler(MessageHandler(Filters.document, handle_document))

    try:
        info = updater.bot.get_webhook_info()
        if info and (info.url or info.pending_update_count):
            updater.bot.delete_webhook(drop_pending_updates=True)
    except Exception as exc:
        logger.info("Webhook reset skipped: %s", exc)

    try:
        updater.start_polling(clean=True, timeout=30, drop_pending_updates=True)
        updater.idle()
    except Conflict as exc:
        logger.error(
            "Telegram 409 Conflict (legacy): %s. Another poller/webhook active.",
            exc,
        )
        try:
            updater.stop()
        except Exception:  # pragma: no cover - defensive cleanup
            pass
        sys.exit(0)
    except (NetworkError, TelegramError) as exc:
        logger.exception("Telegram error (legacy): %s", exc)
        sys.exit(4)


if __name__ == "__main__":
    try:
        with single_instance_lock("telegram-bot-legacy"):
            main()
    except RuntimeError as exc:
        if str(exc).startswith("lock-busy:"):
            print("Another legacy instance is already running. Exit.", file=sys.stderr)
            sys.exit(0)
        raise
