# -*- coding: utf-8 -*-
"""Entry point for the email bot application."""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import traceback
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from telegram import Update
from telegram.error import TelegramError
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    TypeHandler,
    filters,
)

logger = logging.getLogger("email_bot.selfcheck")
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def _selfcheck_email_clean_exports() -> None:
    if os.getenv("EMAILBOT_SKIP_EMAIL_CLEAN_SELFTEST", "0") == "1":
        logger.warning("Selfcheck skipped by EMAILBOT_SKIP_EMAIL_CLEAN_SELFTEST=1")
        return
    required = {
        "canonical_email",
        "parse_emails_unified",
        "dedupe_with_variants",
        "dedupe_keep_original",
        "sanitize_email",
        "finalize_email",
        "normalize_email",
        "repair_email",
        "get_variants",
        "drop_leading_char_twins",
        "drop_trailing_char_twins",
    }
    try:
        import importlib

        module = importlib.import_module("utils.email_clean")
    except Exception as exc:  # pragma: no cover - диагностический путь
        logger.error("[EBOT-SC-001] Failed to import utils.email_clean: %s", exc)
        sys.exit(1)
    missing = sorted(name for name in required if not hasattr(module, name))
    if missing:
        logger.error(
            "[EBOT-SC-002] Missing exports in utils.email_clean: %s\n"
            "Please apply the compatibility patch to utils/email_clean.py.",
            ", ".join(missing),
        )
        sys.exit(1)
    logger.info("[EBOT-SC-OK] utils.email_clean exports are complete.")


_selfcheck_email_clean_exports()

from emailbot import bot_handlers, messaging, history_service
from emailbot import compat  # EBOT-105

compat.apply()  # ранний прогрев совместимости

from emailbot.selfcheck import startup_selfcheck
try:  # Загружаем watchdog-touch и установщик ранним импортом, чтобы не было гонок при запуске
    from emailbot.progress_watchdog import (  # Импортируем PTB-интеграцию watchdog одним блоком
        install as install_watchdog,
        touch as watchdog_touch,
    )
except Exception:  # Не прерываем загрузку, даже если модуль недоступен
    install_watchdog = None  # В тестовом окружении watchdog может отсутствовать
    watchdog_touch = None  # Сохраняем совместимость при отсутствии watchdog

# Default watchdog stall timeout in milliseconds (configurable via env).
WATCHDOG_STALLED_MS = int(os.getenv("WATCHDOG_STALLED_MS", "90000"))
os.environ.setdefault("WATCHDOG_STALLED_MS", str(WATCHDOG_STALLED_MS))

# [EBOT-072] Привязка массового отправителя: жёстко связываем
# штатный send_all с bot_handlers.send_selected, чтобы _resolve_mass_handler()
# сразу получил корректный обработчик без хрупких динамических импортов.
try:
    from emailbot.handlers.manual_send import send_all as _manual_send_all

    setattr(bot_handlers, "send_selected", _manual_send_all)
except Exception as _e:  # pragma: no cover - диагностический путь
    logging.getLogger(__name__).warning(
        "[EBOT-072] Failed to bind mass sender early: %r", _e
    )
from emailbot.services import cooldown as _cooldown
from emailbot.suppress_list import get_blocked_count, init_blocked
from emailbot.config import ENABLE_INLINE_EMAIL_EDITOR
from emailbot.messaging_utils import SecretFilter
from emailbot.utils import load_env

SCRIPT_DIR = Path(__file__).resolve().parent


def _die(msg: str, code: int = 2) -> None:
    try:
        logging.getLogger(__name__).error(msg)
    finally:
        sys.stderr.write(msg + "\n")
        sys.exit(code)


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


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log exceptions raised by PTB handlers with contextual details."""

    logger = logging.getLogger(__name__)
    err = context.error
    try:
        tb = ""
        if err is not None:
            tb = "".join(
                traceback.format_exception(type(err), err, err.__traceback__)
            )
        chat_id = None
        effective_chat = None
        try:
            if isinstance(update, Update):
                effective_chat = update.effective_chat
            elif hasattr(update, "effective_chat"):
                effective_chat = getattr(update, "effective_chat")
            if effective_chat:
                chat_id = getattr(effective_chat, "id", None)
        except Exception:
            chat_id = None
        logger.error(
            "Unhandled exception",
            extra={
                "chat_id": chat_id,
                "update_type": type(update).__name__ if update else None,
                "traceback": tb,
            },
        )
        if chat_id:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="⚠️ Произошла ошибка. Попробуйте ещё раз.",
                )
            except TelegramError:
                pass
            except Exception:
                pass
    except Exception:
        pass


def main() -> None:
    errs = startup_selfcheck()
    if errs:
        _die("Selfcheck failed:\n - " + "\n - ".join(errs))

    load_env(SCRIPT_DIR)

    try:
        history_service.ensure_initialized()
    except Exception:
        logging.getLogger(__name__).debug("history init failed", exc_info=True)

    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    messaging.EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "")
    messaging.EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
    messaging.check_env_vars()

    log_path = SCRIPT_DIR / "bot.log"
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    configure_logging(
        log_path,
        [token, messaging.EMAIL_PASSWORD, messaging.EMAIL_ADDRESS],
    )

    os.makedirs(messaging.DOWNLOAD_DIR, exist_ok=True)
    messaging.dedupe_blocked_file()

    try:
        init_blocked(messaging.BLOCKED_FILE)
        blocked_total = get_blocked_count()
        logging.getLogger(__name__).info(
            "Stoplist loaded", extra={"event": "stoplist", "count": blocked_total}
        )
    except Exception:
        logging.getLogger(__name__).warning("Stoplist init failed", exc_info=True)

    logger = logging.getLogger(__name__)
    try:
        # Логируем критичные пути и версии модулей — это НЕ меняет поведение, но помогает быстро поймать рассинхрон.
        logger.info(
            "[BOOT] Paths: BLOCKED_FILE=%s; SENT_LOG_PATH=%s; SYNC_STATE_PATH=%s; HISTORY_DB=%s",
            getattr(messaging, "BLOCKED_FILE", "?"),
            getattr(messaging, "LOG_FILE", "?"),
            getattr(messaging, "SYNC_STATE_PATH", "?"),
            _cooldown._send_history_path(),
        )
        logger.info(
            "[BOOT] bot_handlers at %s; has start_sending=%s",
            getattr(bot_handlers, "__file__", "?"),
            hasattr(bot_handlers, "start_sending"),
        )
    except Exception as _e:
        logger.warning("[BOOT] path diagnostics failed: %s", _e)

    app = ApplicationBuilder().token(token).build()
    app.add_error_handler(error_handler)

    async def _touch_all(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        """Fix watchdog heartbeat for every update to avoid false positives."""

        if watchdog_touch is None:  # Без watchdog обновлять нечего
            return  # Выходим сразу, чтобы не тратить время
        try:
            watchdog_touch("update")  # Регистрируем поступление любого апдейта
        except Exception:
            pass  # Лишние ошибки не должны мешать обработке апдейта

    if watchdog_touch is not None:  # Подключаем ранний TypeHandler только если watchdog активен
        app.add_handler(  # Регистрируем системный обработчик, работающий раньше остальных
            TypeHandler(Update, _touch_all),
            group=-9999,
        )

    if install_watchdog is not None:  # Конфигурируем фоновые проверки JobQueue, если они доступны
        try:
            install_watchdog(app)  # Запускаем мягкий watchdog поверх PTB JobQueue
        except Exception:
            logging.getLogger(__name__).warning("Watchdog install failed", exc_info=True)

    app.add_handler(CommandHandler("start", bot_handlers.start))
    app.add_handler(CommandHandler("retry_last", bot_handlers.retry_last_command))
    app.add_handler(CommandHandler("diag", bot_handlers.diag))
    app.add_handler(CommandHandler("features", bot_handlers.features))
    app.add_handler(CommandHandler("selfcheck", bot_handlers.selfcheck_command))
    app.add_handler(CommandHandler("url", bot_handlers.url_command))
    app.add_handler(CommandHandler("crawl", bot_handlers.crawl_command))
    app.add_handler(CommandHandler("drop", bot_handlers.handle_drop))

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
        MessageHandler(
            filters.TEXT & filters.Regex("^🩺"), bot_handlers.selfcheck_command
        )
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^🛑"), bot_handlers.stop_process)
    )

    app.add_handler(MessageHandler(filters.Document.ALL, bot_handlers.handle_document))

    bulk_delete_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(
                bot_handlers.bulk_delete_start, pattern="^bulk:delete:start$"
            )
        ],
        states={
            bot_handlers.BULK_DELETE: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    bot_handlers.bulk_delete_text,
                )
            ]
        },
        fallbacks=[],
        per_chat=True,
        per_user=True,
    )
    app.add_handler(bulk_delete_conv, group=-1)
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            bot_handlers.route_text_message,
        ),
        group=5,
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & (~filters.COMMAND), bot_handlers.corrections_text_handler
        )
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND
            & filters.Regex(bot_handlers.URL_REGEX),
            bot_handlers.handle_url_text,
        )
    )
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, bot_handlers.handle_text)
    )

    app.add_handler(
        CallbackQueryHandler(bot_handlers.manual_start, pattern="^manual$"), group=0
    )
    app.add_handler(
        CallbackQueryHandler(
            bot_handlers.manual_select_group, pattern="^manual_group_"
        ),
        group=0,
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.proceed_to_group, pattern="^proceed_group$")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.open_dirs_callback, pattern="^open_dirs$")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.toggle_ignore_180, pattern="^toggle_ignore_180$")
    )
    app.add_handler(
        CallbackQueryHandler(
            bot_handlers.toggle_ignore_180d, pattern="^toggle_ignore_180d$"
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            bot_handlers.enable_text_corrections,
            pattern="^enable_text_corrections$",
        )
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.stop_job_callback, pattern="^stop_job$")
    )
    app.add_handler(CallbackQueryHandler(bot_handlers.select_group, pattern="^group_"))
    app.add_handler(CallbackQueryHandler(bot_handlers.select_group, pattern="^dir:"))
    # единственный валидный старт — по batch_id
    app.add_handler(
        CallbackQueryHandler(bot_handlers.start_sending, pattern="^bulk_start:")
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
        CallbackQueryHandler(bot_handlers.show_skipped_menu, pattern="^skipped_menu$")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.show_skipped_examples, pattern="^skipped:")
    )
    app.add_handler(
        CallbackQueryHandler(
            bot_handlers.ask_include_numeric, pattern="^ask_include_numeric$"
        )
    )
    if ENABLE_INLINE_EMAIL_EDITOR:
        app.add_handler(
            CallbackQueryHandler(bot_handlers.bulk_edit_start, pattern="^bulk:edit:start$")
        )
        app.add_handler(
            CallbackQueryHandler(bot_handlers.bulk_edit_add_prompt, pattern="^bulk:edit:add$")
        )
        app.add_handler(
            CallbackQueryHandler(
                bot_handlers.bulk_edit_replace_prompt, pattern="^bulk:edit:replace$"
            )
        )
        app.add_handler(
            CallbackQueryHandler(
                bot_handlers.bulk_edit_delete, pattern=r"^bulk:edit:del:"
            )
        )
        app.add_handler(
            CallbackQueryHandler(
                bot_handlers.bulk_edit_page, pattern=r"^bulk:edit:page:"
            )
        )
        app.add_handler(
            CallbackQueryHandler(bot_handlers.bulk_edit_done, pattern="^bulk:edit:done$")
        )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.prompt_mass_send, pattern="^bulk:send:start$")
    )
    app.add_handler(
        CallbackQueryHandler(bot_handlers.bulk_txt_start, pattern="^bulk:txt:start$")
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
        if watchdog_touch is not None:  # Перед запуском поллинга делаем явный взмах watchdog'а
            try:
                watchdog_touch("app-start")  # Фиксируем начало работы, чтобы сторож не срабатывал сразу
            except Exception:
                pass  # Лишние ошибки не должны мешать старту бота
        app.run_polling()
    finally:
        stop_event.set()
        t.join()


if __name__ == "__main__":
    main()
