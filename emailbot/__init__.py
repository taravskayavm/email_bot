"""Helpers for the email bot."""

import importlib

from .utils import load_env, log_error, setup_logging

extraction = importlib.import_module(".extraction", __name__)
reporting = importlib.import_module(".reporting", __name__)
try:  # pragma: no cover - optional dependency
    unsubscribe = importlib.import_module(".unsubscribe", __name__)
except Exception:  # pragma: no cover - allow running without aiohttp
    unsubscribe = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from .models import EmailEntry
except Exception:  # pragma: no cover - fallback when models can't be imported
    EmailEntry = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from .smtp_client import SmtpClient
except Exception:  # pragma: no cover - fallback when SMTP client can't be imported
    SmtpClient = None  # type: ignore[assignment]

__all__ = [
    "load_env",
    "setup_logging",
    "log_error",
    "SmtpClient",
    "extraction",
    "messaging",
    "unsubscribe",
    "reporting",
    "EmailEntry",
]


def __getattr__(name: str):
    if name == "messaging":
        return importlib.import_module(f".{name}", __name__)
    raise AttributeError(name)
