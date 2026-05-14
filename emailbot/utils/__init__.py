import logging
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
try:
    # В подпроцессе при spawn импорт пакета может происходить в «урезанном» окружении.
    # Если telegram недоступен, не валим импорт целиком — подставим минимум по месту.
    from telegram import constants  # type: ignore
except Exception:  # pragma: no cover - light fallback
    class _LightMessageLimit:
        MAX_TEXT_LENGTH = 4096


    class _LightConstants:
        # Используется только для разбиения длинных сообщений/ограничений длины.
        # Подберите реальные значения из PTB, если библиотека установлена.
        MessageLimit = _LightMessageLimit()
        # Совместимость: некоторые места ожидают именованный атрибут MAX_MESSAGE_LENGTH
        MAX_MESSAGE_LENGTH = 4096

    constants = _LightConstants()  # type: ignore


logger = logging.getLogger(__name__)


def _chunk_text(text: str, limit: int) -> list[str]:
    """Split ``text`` into Telegram-friendly chunks."""

    if len(text) <= limit:
        return [text]
    parts: list[str] = []
    start = 0
    while start < len(text):
        end = min(start + limit, len(text))
        newline = text.rfind("\n", start, end)
        if newline == -1 or newline < start + int(limit * 0.5):
            newline = end
        parts.append(text[start:newline])
        start = newline
    return [part for part in parts if part]


async def safe_send_message(bot, chat_id: int, text: str, **kwargs):
    """Send ``text`` in chunks to avoid Telegram 4096 limit."""

    limit = constants.MessageLimit.MAX_TEXT_LENGTH
    first = True
    for chunk in _chunk_text(text, limit):
        if first:
            await bot.send_message(chat_id=chat_id, text=chunk, **kwargs)
            first = False
        else:
            follow_kwargs = {k: v for k, v in kwargs.items() if k != "reply_markup"}
            await bot.send_message(chat_id=chat_id, text=chunk, **follow_kwargs)


def load_env(script_dir: Path) -> None:
    """Load environment variables from .env files."""
    try:
        load_dotenv(dotenv_path=script_dir / ".env")
        load_dotenv()
    except Exception as exc:
        logger.debug("load_env failed: %r", exc)


def setup_logging(log_file: Path) -> None:
    """Configure basic logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


def log_error(msg: str) -> None:
    """Log an error message and append it to ``bot_errors.log``."""
    logger.error(msg)
    try:
        err_file = Path(__file__).resolve().parent / "bot_errors.log"
        with err_file.open("a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()} {msg}\n")
    except Exception as exc:
        logger.debug("log_error append failed: %r", exc)


try:  # pragma: no cover - optional bridge for legacy imports
    from . import utils_preview_export as _preview_export

    sys.modules[__name__ + ".preview_export"] = _preview_export
except Exception as exc:  # pragma: no cover - ignore if optional dependency missing
    logger.debug("preview_export bridge not available: %r", exc)
