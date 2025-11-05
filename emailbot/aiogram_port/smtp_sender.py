"""Simple SMTP sending helper used by the aiogram entrypoint."""

from __future__ import annotations

import asyncio
import os
import smtplib
import ssl
from email.message import EmailMessage
from functools import partial
from typing import Optional


def _get_setting(name: str, default=None):
    value = os.getenv(name)
    if value is not None:
        return value
    try:
        import emailbot.settings as settings_module  # type: ignore

        if hasattr(settings_module, name):
            return getattr(settings_module, name)
    except Exception:
        pass
    return default


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "on"}


class SmtpSender:
    """Minimal SMTP client with optional STARTTLS support."""

    def __init__(self) -> None:
        self.host = str(_get_setting("SMTP_HOST", "smtp.mail.ru"))
        self.port = int(_get_setting("SMTP_PORT", 465))
        self.starttls = _as_bool(_get_setting("SMTP_STARTTLS", self.port != 465))
        self.email_address = _get_setting("EMAIL_ADDRESS")
        self.email_password = _get_setting("EMAIL_PASSWORD")
        self.timeout = int(_get_setting("SMTP_TIMEOUT", 20))
        self._ssl_context = ssl.create_default_context()
        self._ssl_context.check_hostname = True
        self._ssl_context.verify_mode = ssl.CERT_REQUIRED
        if not self.email_address or not self.email_password:
            raise RuntimeError("EMAIL_ADDRESS/EMAIL_PASSWORD must be configured")

    def _connect(self):
        if not self.starttls and self.port == 465:
            client = smtplib.SMTP_SSL(
                self.host,
                self.port,
                timeout=self.timeout,
                context=self._ssl_context,
            )
        else:
            client = smtplib.SMTP(self.host, self.port, timeout=self.timeout)
            client.ehlo()
            try:
                client.starttls(context=self._ssl_context)
                client.ehlo()
            except smtplib.SMTPNotSupportedError:
                pass
        client.login(str(self.email_address), str(self.email_password))
        return client

    def send(
        self,
        *,
        to_addr: str,
        subject: str,
        body: str,
        html: Optional[str] = None,
    ) -> None:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = str(self.email_address)
        msg["To"] = to_addr
        if html:
            msg.set_content(body or "")
            msg.add_alternative(html, subtype="html")
        else:
            msg.set_content(body or "")

        self.send_message(msg)

    def send_message(self, msg: EmailMessage) -> None:
        """Synchronous send (kept for compatibility)."""
        client = self._connect()
        try:
            client.send_message(msg)
        finally:
            try:
                client.quit()
            except Exception:
                pass

    async def send_message_async(self, msg: EmailMessage) -> None:
        """Run the blocking send in an executor to avoid blocking the event loop."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.send_message, msg)

    async def send_async(
        self,
        *,
        to_addr: str,
        subject: str,
        body: str,
        html: Optional[str] = None,
    ) -> None:
        """Asynchronous counterpart for :meth:`send`."""

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            partial(self.send, to_addr=to_addr, subject=subject, body=body, html=html),
        )
