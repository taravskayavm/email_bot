"""
EBOT-089: Универсальный слой совместимости.
Подставляет безопасные заглушки/переименованные атрибуты,
если в проекте ещё остались старые обращения.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional


log = logging.getLogger(__name__)


def _normalize_key(val: Any) -> Optional[int | str]:
    # такая же реализация, как в messaging._normalize_key (EBOT-088)
    if val is None:
        return None
    try:
        if isinstance(val, int):
            return val
        chat_id = getattr(val, "chat_id", None)
        if isinstance(chat_id, int):
            return chat_id
        chat = getattr(val, "chat", None)
        if chat is not None:
            cid = getattr(chat, "id", None)
            if isinstance(cid, int):
                return cid
        obj_id = getattr(val, "id", None)
        if isinstance(obj_id, int):
            return obj_id
        s = str(val).strip()
        if s.isdigit():
            try:
                return int(s)
            except Exception:
                pass
        return s
    except Exception:
        try:
            return int(str(val).strip())
        except Exception:
            return str(val).strip()


def apply() -> None:
    """
    Применяет швы совместимости:
    - добавляет messaging._normalize_key, если отсутствует;
    - добавляет settings.PARSE_FILE_TIMEOUT/SEND_FILE_TIMEOUT по нужным правилам (если нет);
    - при необходимости — no-op раннер для отправки сообщений из чужого потока.
    """
    try:
        from . import messaging

        if not hasattr(messaging, "_normalize_key"):
            setattr(messaging, "_normalize_key", _normalize_key)
            log.info("compat: injected messaging._normalize_key")

        if not hasattr(messaging, "run_in_app_loop"):
            # на всякий случай
            def run_in_app_loop(application, coro):
                return asyncio.run_coroutine_threadsafe(coro, application.loop)

            setattr(messaging, "run_in_app_loop", run_in_app_loop)
            log.info("compat: injected messaging.run_in_app_loop")
    except Exception as e:
        log.debug("compat: messaging shim skipped: %r", e)

    try:
        from . import settings

        # Таймауты: SEND_FILE_TIMEOUT -> PARSE_FILE_TIMEOUT -> FILE_TIMEOUT -> 20
        if not hasattr(settings, "SEND_FILE_TIMEOUT"):
            val = int(getattr(settings, "FILE_TIMEOUT", 20))
            setattr(settings, "SEND_FILE_TIMEOUT", val)
            log.info("compat: injected settings.SEND_FILE_TIMEOUT=%s", val)

        if not hasattr(settings, "PARSE_FILE_TIMEOUT"):
            val = int(getattr(settings, "SEND_FILE_TIMEOUT", 20))
            setattr(settings, "PARSE_FILE_TIMEOUT", val)
            log.info("compat: injected settings.PARSE_FILE_TIMEOUT=%s", val)
    except Exception as e:
        log.debug("compat: settings shim skipped: %r", e)
