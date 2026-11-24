"""Save Sent copies independently of SMTP delivery, with bounded recovery."""
from __future__ import annotations

import imaplib
import logging
import time
from email import policy
from email.parser import BytesParser

logger = logging.getLogger(__name__)


class SentCopySession:
    def __init__(self, connection, connect, resolve_folder):
        self.connection = connection
        self.connect = connect
        self.resolve_folder = resolve_folder
        self.failed_recipients: list[str] = []

    def _discard(self):
        connection, self.connection = self.connection, None
        if connection is not None:
            try:
                connection.shutdown()
            except Exception:
                logger.debug("IMAP socket shutdown failed", exc_info=True)

    def logout(self):
        if self.connection is not None:
            try:
                self.connection.logout()
            except Exception:
                logger.warning("IMAP logout failed after dispatch", exc_info=True)
            finally:
                self._discard()

    def save(self, raw: bytes, folder: str | None = None) -> bool:
        message = BytesParser(policy=policy.default).parsebytes(raw, headersonly=True)
        message_id = str(message.get("Message-ID", ""))
        recipient = str(message.get("To", "неизвестный адрес"))
        append_started = False
        for attempt in range(2):
            try:
                if self.connection is None:
                    self.connection = self.connect()
                connection = self.connection
                if folder is None:
                    folder = self.resolve_folder(connection)
                if hasattr(connection, "select"):
                    status, _ = connection.select(folder)
                    if status != "OK":
                        raise imaplib.IMAP4.error(f"Cannot select Sent folder: {status}")
                # APPEND may have succeeded before its response was lost.
                # Confirm by Message-ID before retrying the same copy.
                if attempt and append_started:
                    if not message_id:
                        raise imaplib.IMAP4.error("Cannot confirm APPEND without Message-ID")
                    quoted_id = '"' + message_id.replace("\\", "\\\\").replace('"', '\\"') + '"'
                    status, data = connection.uid("SEARCH", None, "HEADER", "Message-ID", quoted_id)
                    if status != "OK":
                        raise imaplib.IMAP4.error("Cannot confirm previous APPEND")
                    if data and any(item for item in data if item):
                        return True
                append_started = True
                status, data = connection.append(
                    folder, "(\\Seen)", imaplib.Time2Internaldate(time.time()), raw
                )
                if status != "OK":
                    append_started = False  # Server explicitly rejected this copy.
                    raise imaplib.IMAP4.error(f"APPEND rejected: {status} {data}")
                logger.info("Sent copy saved for %s", recipient)
                return True
            except Exception:
                logger.warning("Sent copy attempt %s failed for %s", attempt + 1, recipient, exc_info=True)
                self._discard()
        self.failed_recipients.append(recipient)
        return False

    def failure_report(self) -> str:
        if not self.failed_recipients:
            return ""
        addresses = "\n".join(self.failed_recipients)
        return (
            f"⚠️ Письма переданы почтовому серверу, но сохранение копий в «Отправленные» "
            f"не подтверждено: {len(self.failed_recipients)}.\n"
            "Повторное подключение не помогло. Эти адреса остаются в истории отправок, "
            "чтобы не отправить письма повторно. Это не подтверждение доставки получателям.\n"
            + addresses
        )
