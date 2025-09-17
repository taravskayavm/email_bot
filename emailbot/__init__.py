"""Helpers for the email bot."""

from . import bot_handlers, extraction, messaging, unsubscribe, reporting
from .models import EmailEntry
from .smtp_client import SmtpClient
from .utils import load_env, log_error, setup_logging

__all__ = [
    "load_env",
    "setup_logging",
    "log_error",
    "SmtpClient",
    "extraction",
    "messaging",
    "bot_handlers",
    "unsubscribe",
    "reporting",
    "EmailEntry",
]
