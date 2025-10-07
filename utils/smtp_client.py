"""Compatibility layer re-exporting the canonical SMTP helpers."""

from emailbot.smtp_client import RobustSMTP, SmtpClient, send_with_retry

__all__ = ["RobustSMTP", "SmtpClient", "send_with_retry"]
