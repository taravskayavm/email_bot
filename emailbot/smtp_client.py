import os
import smtplib
import ssl
from typing import Optional


class SmtpClient:
    """Simple SMTP client with context manager support."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        use_ssl: Optional[bool] = None,
        timeout: int = 15,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        if use_ssl is None:
            use_ssl = os.getenv("SMTP_SSL", "0") == "1"
        self.use_ssl = use_ssl
        self.timeout = timeout
        self._server: Optional[smtplib.SMTP] = None

    def __enter__(self) -> "SmtpClient":
        context = ssl.create_default_context()
        if self.use_ssl:
            self._server = smtplib.SMTP_SSL(
                self.host, self.port, timeout=self.timeout, context=context
            )
        else:
            self._server = smtplib.SMTP(
                self.host, self.port, timeout=self.timeout
            )
            self._server.starttls(context=context)
        self._server.login(self.username, self.password)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._server is not None:
            try:
                self._server.quit()
            finally:
                self._server = None

    def send(self, sender: str, recipient: str, raw_message: str) -> None:
        if self._server is None:
            raise RuntimeError("SMTP client is not connected")
        self._server.sendmail(sender, recipient, raw_message)
