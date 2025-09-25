"""Middlewares for the emailbot Telegram bot."""

from .error_logging import ErrorLoggingMiddleware

__all__ = ["ErrorLoggingMiddleware"]
