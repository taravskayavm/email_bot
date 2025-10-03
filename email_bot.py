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
            "time": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            data["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(data, ensure_ascii=False)


class SecretFilter(logging.Filter):
    """Mask sensitive values in logs."""

    def __init__(self, secrets: list[str]) -> None:
        super().__init__()
        self.secrets = [s for s in secrets if s]

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        msg = record.getMessage()
        for s in self.secrets:
            if s and s in msg:
                record.msg = msg.replace(s, "****")
        return True


def configure_logging() -> None:
    """Set up console + rotating file logging with JSON formatter."""
    log_dir = SCRIPT_DIR / "var"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "email_bot.log"

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    stream = logging.StreamHandler()
    stream.setFormatter(JsonFormatter())

    file_handler = TimedRotatingFileHandler(
        log_file, when="midnight", backupCount=7, encoding="utf-8"
    )
    file_handler.setFormatter(JsonFormatter())

    # Mask secrets we know from env
    secrets = [
        os.getenv("TELEGRAM_BOT_TOKEN", ""),
        os.getenv("EMAIL_ADDRESS", ""),
        os.getenv("EMAIL_PASSWORD", ""),
    ]
    root.addFilter(SecretFilter(secrets))
    root.addHandler(stream)
    root.addHandler(file_handler)


def main() -> None:
    load_env(SCRIPT_DIR)
    configure_logging()

    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    messaging.EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "")
    messaging.EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
    messaging.check_env_vars()
    messaging.dedupe_blocked_file()

    app = ApplicationBuilder().token(token).build()

    # Commands
    app.add_handler(CommandHandler("start", bot_handlers.start))
    app.add_handler(CommandHandler("retry_last", bot_handlers.retry_last_command))
    app.add_handler(CommandHandler("diag", bot_handlers.diag))
    app.add_handler(CommandHandler("features", bot_handlers.features))

    # Text buttons
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^📤"), bot_handlers.prompt_upload)
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex("^📧"), bot_handlers.prompt_email_settings
        )
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^⚙️"), bot_handlers.prompt_settings)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^🧹"), bot_handlers.clean_state)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^🚀"), bot_handlers.force_send_command)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^🛑"), bot_handlers.stop_process)
    )

    # Documents / any text
    app.add_handler(MessageHandler(filters.Document.ALL, bot_handlers.handle_document))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, bot_handlers.handle_text)
    )

    # Inline callbacks
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

    # Background periodic tasks (unsubscribe checks, etc.)
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
