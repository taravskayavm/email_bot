# -*- coding: utf-8 -*-
"""Entry point for the email bot application."""

from __future__ import annotations

import os
import threading
from pathlib import Path

from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from emailbot.utils import load_env, setup_logging
from emailbot import messaging, bot_handlers

SCRIPT_DIR = Path(__file__).resolve().parent


def main() -> None:
    setup_logging(SCRIPT_DIR / "bot.log")
    load_env(SCRIPT_DIR)

    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    messaging.EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "")
    messaging.EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
    messaging.check_env_vars()

    os.makedirs(messaging.DOWNLOAD_DIR, exist_ok=True)
    messaging.dedupe_blocked_file()

    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", bot_handlers.start))

    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^📤"), bot_handlers.prompt_upload)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^🧹"), bot_handlers.reset_email_list)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^🧾"), bot_handlers.about_bot)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^🚫"), bot_handlers.add_block_prompt)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^📄"), bot_handlers.show_blocked_list)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^✉️"), bot_handlers.prompt_manual_email)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^🧭"), bot_handlers.prompt_change_group)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^📈"), bot_handlers.report_command)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^🔄"), bot_handlers.sync_imap_command)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^🚀"), bot_handlers.force_send_command)
    )

    app.add_handler(MessageHandler(filters.Document.ALL, bot_handlers.handle_document))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, bot_handlers.handle_text)
    )

    app.add_handler(
        CallbackQueryHandler(bot_handlers.send_manual_email, pattern="^manual_group_")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.proceed_to_group, pattern="^proceed_group$")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.select_group, pattern="^group_")
    )
    app.add_handler(CallbackQueryHandler(bot_handlers.send_all, pattern="^start_sending"))
    app.add_handler(CallbackQueryHandler(bot_handlers.report_callback, pattern="^report_"))
    app.add_handler(
        CallbackQueryHandler(bot_handlers.show_numeric_list, pattern="^show_numeric$")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.show_foreign_list, pattern="^show_foreign$")
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
