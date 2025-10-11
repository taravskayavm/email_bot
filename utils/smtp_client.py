"""Compatibility layer re-exporting the canonical SMTP helpers.

Оставляем модуль для обратной совместимости, но мягко предупреждаем,
что нужно импортировать из :mod:`emailbot.smtp_client`.
"""
from __future__ import annotations

import warnings

from emailbot.smtp_client import RobustSMTP, SmtpClient, send_with_retry  # canonical

__all__ = ["RobustSMTP", "SmtpClient", "send_with_retry"]

# Мягкое предупреждение при первом обращении к совместимостному модулю.
warnings.warn(
    "utils.smtp_client устарел: используйте emailbot.smtp_client",
    DeprecationWarning,
    stacklevel=2,
)
