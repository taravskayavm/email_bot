"""Compatibility aliases for legacy messaging helpers."""

from __future__ import annotations

try:  # pragma: no cover - import is straightforward
    from .messaging import send_email as send_mail
except Exception:  # pragma: no cover - minimal stub for broken environments
    def send_mail(*args, **kwargs):  # type: ignore[return-type]
        raise NotImplementedError("legacy send_mail unavailable")


try:  # pragma: no cover - import is straightforward
    from .messaging import send_email_with_sessions as send_batch
except Exception:  # pragma: no cover - minimal stub for broken environments
    def send_batch(*args, **kwargs):  # type: ignore[return-type]
        raise NotImplementedError("legacy send_batch unavailable")


try:  # pragma: no cover - import is straightforward
    from .smtp_client import SmtpClient as LegacySMTPClient
except Exception:  # pragma: no cover - provide lightweight shim
    class LegacySMTPClient:  # type: ignore[too-many-ancestors]
        def __init__(self, *args, **kwargs) -> None:
            raise NotImplementedError("legacy SMTP client unavailable")


__all__ = ["send_mail", "send_batch", "LegacySMTPClient"]
