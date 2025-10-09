from __future__ import annotations

import io
import os
import re
from typing import Dict, List

import requests
from telegram import ParseMode
from telegram.ext import CommandHandler, Filters, MessageHandler, Updater

from emailbot.messaging_utils import is_blocked, is_suppressed
from emailbot.pipelines.ingest import ingest_emails
from emailbot.utils.email_clean import clean_and_normalize_email
from emailbot.utils.file_email_extractor import ExtractError, extract_emails_from_bytes

URL_RE = re.compile(r"(https?://[^\s]+)", re.IGNORECASE)
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}")


def start(update, context):
    update.message.reply_text("📥 Загрузите данные с e-mail-адресами или пришлите ссылку.")


def _render_stats(ok: List[str], rejects: Dict[str, int], warn: str | None = None, errors: List[str] | None = None) -> str:
    txt = f"Найдено адресов: {len(ok)}"
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
                if is_blocked(email) or is_suppressed(email):
                    continue
                total_ok.append(email)
        except Exception as exc:  # pragma: no cover - network errors
            errors.append(f"{url} — {type(exc).__name__}")

    total_ok = list(dict.fromkeys(total_ok))
    ack.edit_text(
        _render_stats(total_ok, total_rejects, errors=errors),
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
        ok = [addr for addr in ok if not (is_blocked(addr) or is_suppressed(addr))]
        ok = list(dict.fromkeys(ok))
        ack.edit_text(_render_stats(ok, rejects, warn=warn), parse_mode=ParseMode.MARKDOWN)
    except ExtractError as exc:
        ack.edit_text(f"Не удалось обработать файл: {exc}")
    except Exception:  # pragma: no cover - defensive
        ack.edit_text("Произошла ошибка при разборе файла.")


def main():
    updater = Updater(token=os.environ["TELEGRAM_BOT_TOKEN"], use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("ingest", ingest))
    dp.add_handler(MessageHandler(Filters.text & (~Filters.command), handle_url))
    dp.add_handler(MessageHandler(Filters.document, handle_document))

    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
