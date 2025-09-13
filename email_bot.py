# -*- coding: utf-8 -*-
"""Entry point for the email bot application."""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)

from emailbot import bot_handlers, messaging
from emailbot.messaging_utils import SecretFilter
from emailbot.utils import load_env

SCRIPT_DIR = Path(__file__).resolve().parent


class JsonFormatter(logging.Formatter):
    """Format logs as JSON objects."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        data = {
            "time": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        for key in ("event", "email", "source", "code", "phase", "count"):
            if key in record.__dict__:
                data[key] = record.__dict__[key]
        return json.dumps(data, ensure_ascii=False)


class SizedTimedRotatingFileHandler(TimedRotatingFileHandler):
    """Rotate logs daily and when exceeding a size threshold."""

    def __init__(self, filename: Path, maxBytes: int = 1_000_000, **kwargs):
        super().__init__(filename, **kwargs)
        self.maxBytes = maxBytes

    def shouldRollover(self, record: logging.LogRecord) -> int:  # type: ignore[override]
        if super().shouldRollover(record):
            return 1
        if self.maxBytes > 0:
            self.stream = self.stream or self._open()
            self.stream.seek(0, os.SEEK_END)
            if self.stream.tell() >= self.maxBytes:
                return 1
        return 0


def configure_logging(log_file: Path, secrets: list[str]) -> None:
    formatter = JsonFormatter()
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    stream = logging.StreamHandler()
    stream.setFormatter(formatter)

    file_handler = SizedTimedRotatingFileHandler(
        log_file, when="midnight", backupCount=7, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    root.addHandler(stream)
    root.addHandler(file_handler)
    root.addFilter(SecretFilter(secrets))


def main() -> None:
    load_env(SCRIPT_DIR)

    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    messaging.EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "")
    messaging.EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
    messaging.check_env_vars()

    configure_logging(
        SCRIPT_DIR / "bot.log",
        [token, messaging.EMAIL_PASSWORD, messaging.EMAIL_ADDRESS],
    )

    os.makedirs(messaging.DOWNLOAD_DIR, exist_ok=True)
    messaging.dedupe_blocked_file()

    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", bot_handlers.start))
    app.add_handler(CommandHandler("retry_last", bot_handlers.retry_last_command))
    app.add_handler(CommandHandler("diag", bot_handlers.diag))
    app.add_handler(CommandHandler("features", bot_handlers.features))
    app.add_handler(CommandHandler("reports", bot_handlers.handle_reports))
    app.add_handler(CommandHandler("reports_debug", bot_handlers.handle_reports_debug))

    # Inline-кнопки для подозрительных адресов
    app.add_handler(
        CallbackQueryHandler(bot_handlers.on_accept_suspects, pattern="^accept_suspects$")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.on_edit_suspects, pattern="^edit_suspects$")
    )
    app.add_handler(
        ConversationHandler(
            entry_points=[],
            states={
                bot_handlers.EDIT_SUSPECTS_INPUT: [
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND,
                        bot_handlers.on_edit_suspects_input,
                    )
                ]
            },
            fallbacks=[],
            name="edit_suspects_flow",
            persistent=False,
        )
    )

    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^📤"), bot_handlers.prompt_upload)
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex("^🧹"), bot_handlers.reset_email_list
        )
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^🧾"), bot_handlers.about_bot)
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex("^🚫"), bot_handlers.add_block_prompt
        )
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex("^📄"), bot_handlers.show_blocked_list
        )
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex("^✉️"), bot_handlers.prompt_manual_email
        )
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex("^🧭"), bot_handlers.prompt_change_group
        )
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^📈"), bot_handlers.report_command)
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex("^📁"), bot_handlers.imap_folders_command
        )
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex("^🔄"), bot_handlers.sync_imap_command
        )
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex("^🚀"), bot_handlers.force_send_command
        )
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^🛑"), bot_handlers.stop_process)
    )

    app.add_handler(MessageHandler(filters.Document.ALL, bot_handlers.handle_document))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, bot_handlers.handle_text)
    )

    app.add_handler(
        CallbackQueryHandler(bot_handlers.manual_mode, pattern="^manual_mode_")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.manual_reset, pattern="^manual_reset$")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.send_manual_email, pattern="^manual_group_")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.proceed_to_group, pattern="^proceed_group$")
    )
    app.add_handler(CallbackQueryHandler(bot_handlers.select_group, pattern="^group_"))
    app.add_handler(
        CallbackQueryHandler(bot_handlers.send_all, pattern="^start_sending")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.report_callback, pattern="^report_")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.show_numeric_list, pattern="^show_numeric$")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.show_foreign_list, pattern="^show_foreign$")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.features_callback, pattern="^feature_")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.refresh_preview, pattern="^refresh_preview$")
    )
    app.add_handler(
        CallbackQueryHandler(
            bot_handlers.ask_include_numeric, pattern="^ask_include_numeric$"
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            bot_handlers.include_numeric_emails, pattern="^confirm_include_numeric$"
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            bot_handlers.cancel_include_numeric, pattern="^cancel_include_numeric$"
        )
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.apply_repairs, pattern="^apply_repairs$")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.show_repairs, pattern="^show_repairs$")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.imap_page_callback, pattern="^imap_page:")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.choose_imap_folder, pattern="^imap_choose:")
    )

    print("Бот запущен.")
    stop_event = threading.Event()
    t = threading.Thread(
        target=messaging.periodic_unsubscribe_check, args=(stop_event,), daemon=True
    )
    t.start()
    try:
        app.run_polling()
    finally:
        stop_event.set()
        t.join()


if __name__ == "__main__":
    main()
