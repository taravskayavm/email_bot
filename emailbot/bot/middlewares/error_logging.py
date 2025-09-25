from __future__ import annotations

import logging

from aiogram import BaseMiddleware

logger = logging.getLogger(__name__)


class ErrorLoggingMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            return await handler(event, data)
        except Exception:
            logger.exception("Unhandled error while processing update")
            raise
