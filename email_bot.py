# -*- coding: utf-8 -*-
"""Entry point for the email bot application."""

from __future__ import annotations

import logging
import os
import threading
import warnings
from pathlib import Path

from telegram import Update
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from emailbot import bot_handlers, messaging
from emailbot.handlers.manual_send import handle_send_flow_actions
from emailbot.messaging_utils import SecretFilter
from emailbot.utils import load_env

# Определяем путь для логов
SCRIPT_DIR = Path(__file__).resolve().parent
LOG_FILE = SCRIPT_DIR / "bot.log"

# Базовая настройка логирования
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
    force=True,
)

warnings.filterwarnings(
    "ignore", category=DeprecationWarning, message="builtin type SwigPy"
)

# Подавляем болтливые логи HTTP-библиотек
for noisy in ("httpx", "httpcore", "urllib3", "aiohttp", "requests"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def _safe_add(app, handler, signature: str) -> None:
    """Register ``handler`` only once per application."""

    seen = app.bot_data.setdefault("_handlers_signatures", set())
    if signature in seen:
        logger.debug("skip duplicate handler: %s", signature)
        return
    seen.add(signature)
    app.add_handler(handler)


async def _on_error(
    update: Update | None, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Глобальный перехватчик ошибок для Application."""

    err = context.error
    if isinstance(err, BadRequest):
        message = str(err)
        if "Query is too old" in message or "query id is invalid" in message:
            logger.info("Callback ignored: %s", message)
            return
    logger.exception("Unhandled error: %s", err)


def main() -> None:
    load_env(SCRIPT_DIR)

    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    messaging.EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "")
    messaging.EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
    messaging.check_env_vars()

    root_logger = logging.getLogger()
    for existing in list(root_logger.filters):
        if isinstance(existing, SecretFilter):
            root_logger.removeFilter(existing)
    root_logger.addFilter(
        SecretFilter([token, messaging.EMAIL_PASSWORD, messaging.EMAIL_ADDRESS])
    )

    os.makedirs(messaging.DOWNLOAD_DIR, exist_ok=True)
    messaging.dedupe_blocked_file()

    app = Application.builder().token(token).build()
    app.bot_data.setdefault("locks", {})

    _safe_add(app, CommandHandler("start", bot_handlers.start), "cmd:start")
    _safe_add(
        app,
        CommandHandler("retry_last", bot_handlers.retry_last_command),
        "cmd:retry_last",
    )
    _safe_add(app, CommandHandler("diag", bot_handlers.diag), "cmd:diag")
    _safe_add(
        app, CommandHandler("features", bot_handlers.features), "cmd:features"
    )
    _safe_add(
        app, CommandHandler("reports", bot_handlers.handle_reports), "cmd:reports"
    )
    _safe_add(
        app,
        CommandHandler("reports_debug", bot_handlers.handle_reports_debug),
        "cmd:reports_debug",
    )

    # Inline-кнопки для подозрительных адресов
    _safe_add(
        app,
        CallbackQueryHandler(
            bot_handlers.on_accept_suspects, pattern="^accept_suspects$"
        ),
        "cb:accept_suspects",
    )
    _safe_add(
        app,
        CallbackQueryHandler(
            bot_handlers.on_edit_suspects, pattern="^edit_suspects$"
        ),
        "cb:edit_suspects",
    )
    _safe_add(
        app,
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
        ),
        "conv:edit_suspects_flow",
    )

    _safe_add(
        app,
        MessageHandler(filters.TEXT & filters.Regex("^📤"), bot_handlers.prompt_upload),
        "msg:prompt_upload",
    )
    _safe_add(
        app,
        MessageHandler(
            filters.TEXT & filters.Regex("^🧹"), bot_handlers.reset_email_list
        ),
        "msg:reset_email_list",
    )
    _safe_add(
        app,
        MessageHandler(filters.TEXT & filters.Regex("^🧾"), bot_handlers.about_bot),
        "msg:about_bot",
    )
    _safe_add(
        app,
        MessageHandler(
            filters.TEXT & filters.Regex("^🚫"), bot_handlers.add_block_prompt
        ),
        "msg:add_block_prompt",
    )
    _safe_add(
        app,
        MessageHandler(
            filters.TEXT & filters.Regex("^📄"), bot_handlers.show_blocked_list
        ),
        "msg:show_blocked_list",
    )
    _safe_add(
        app,
        MessageHandler(
            filters.TEXT & filters.Regex("^✉️"), bot_handlers.prompt_manual_email
        ),
        "msg:prompt_manual_email",
    )
    _safe_add(
        app,
        MessageHandler(
            filters.TEXT & filters.Regex("^🧭"), bot_handlers.prompt_change_group
        ),
        "msg:prompt_change_group",
    )
    _safe_add(
        app,
        MessageHandler(
            filters.TEXT & filters.Regex("^📈"), bot_handlers.report_command
        ),
        "msg:report_command",
    )
    _safe_add(
        app,
        MessageHandler(
            filters.TEXT & filters.Regex("^📁"), bot_handlers.imap_folders_command
        ),
        "msg:imap_folders_command",
    )
    _safe_add(
        app,
        MessageHandler(
            filters.TEXT & filters.Regex("^🔄"), bot_handlers.sync_imap_command
        ),
        "msg:sync_imap_command",
    )
    _safe_add(
        app,
        MessageHandler(
            filters.TEXT & filters.Regex("^🔁"), bot_handlers.sync_bounces_command
        ),
        "msg:sync_bounces_command",
    )
    _safe_add(
        app,
        MessageHandler(
            filters.TEXT & filters.Regex("^🚀"), bot_handlers.force_send_command
        ),
        "msg:force_send_command",
    )
    _safe_add(
        app,
        MessageHandler(filters.TEXT & filters.Regex("^🛑"), bot_handlers.stop_process),
        "msg:stop_process",
    )

    _safe_add(
        app,
        MessageHandler(filters.Document.ALL, bot_handlers.handle_document),
        "msg:handle_document",
    )
    _safe_add(
        app,
        MessageHandler(filters.TEXT & ~filters.COMMAND, bot_handlers.handle_text),
        "msg:handle_text",
    )

    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.manual_mode, pattern="^manual_mode_"),
        "cb:manual_mode",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.manual_reset, pattern="^manual_reset$"),
        "cb:manual_reset",
    )
    _safe_add(
        app,
        CallbackQueryHandler(
            bot_handlers.manual_ignore_selected, pattern="^manual_ignore_selected"
        ),
        "cb:manual_ignore_selected",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.send_manual_email, pattern="^manual_tpl:"),
        "cb:manual_tpl",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.proceed_to_group, pattern="^proceed_group$"),
        "cb:proceed_group",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.select_group, pattern="^tpl:"),
        "cb:select_group",
    )
    _safe_add(
        app,
        CallbackQueryHandler(
            handle_send_flow_actions, pattern=r"^bulk:send:(start|back|edit)$"
        ),
        "cb:bulk_send_flow",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.send_all, pattern="^start_sending"),
        "cb:start_sending",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.report_callback, pattern="^report_"),
        "cb:report_callback",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.show_foreign_list, pattern="^show_foreign$"),
        "cb:show_foreign_list",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.features_callback, pattern="^feature_"),
        "cb:features_callback",
    )
    _safe_add(
        app,
        CallbackQueryHandler(
            bot_handlers.refresh_preview, pattern="^refresh_preview$"
        ),
        "cb:refresh_preview",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.preview_go_back, pattern="^preview_back$"),
        "cb:preview_back",
    )
    _safe_add(
        app,
        CallbackQueryHandler(
            bot_handlers.preview_request_edit, pattern="^preview_edit$"
        ),
        "cb:preview_edit",
    )
    _safe_add(
        app,
        CallbackQueryHandler(
            bot_handlers.preview_show_edits, pattern="^preview_edits_show$"
        ),
        "cb:preview_show_edits",
    )
    _safe_add(
        app,
        CallbackQueryHandler(
            bot_handlers.preview_reset_edits, pattern="^preview_edits_reset$"
        ),
        "cb:preview_reset_edits",
    )
    _safe_add(
        app,
        CallbackQueryHandler(
            bot_handlers.preview_refresh_choice, pattern="^preview_refresh:"
        ),
        "cb:preview_refresh_choice",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.request_fix, pattern=r"^fix:\d+$"),
        "cb:request_fix",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.apply_repairs, pattern="^apply_repairs$"),
        "cb:apply_repairs",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.show_repairs, pattern="^show_repairs$"),
        "cb:show_repairs",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.imap_page_callback, pattern="^imap_page:"),
        "cb:imap_page",
    )
    _safe_add(
        app,
        CallbackQueryHandler(bot_handlers.choose_imap_folder, pattern="^imap_choose:"),
        "cb:imap_choose",
    )

    # Глобальный обработчик ошибок — в самом конце, после регистрации хендлеров
    app.add_error_handler(_on_error)

    logger.info("Бот запущен.")
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
