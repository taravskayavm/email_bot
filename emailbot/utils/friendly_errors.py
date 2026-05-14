"""Map low-level exceptions to user-friendly messages."""

from __future__ import annotations

from smtplib import (
    SMTPAuthenticationError,
    SMTPConnectError,
    SMTPServerDisconnected,
    SMTPSenderRefused,
)
from socket import timeout as SocketTimeout

from .errors import UserError


_MAP: dict[type[BaseException], str] = {
    SMTPAuthenticationError: "SMTP отверг логин/пароль. Проверьте учетные данные.",
    SMTPConnectError: "Не удалось подключиться к SMTP-серверу. Проверьте хост/порт и доступ.",
    SMTPServerDisconnected: "Соединение с SMTP разорвано. Повторите позже или проверьте настройки.",
    SMTPSenderRefused: "SMTP отверг адрес отправителя. Проверьте поле From.",
    SocketTimeout: "Время ожидания соединения истекло. Проверьте сеть/фаервол.",
    ConnectionRefusedError: "SMTP не принимает соединение (Connection refused). Проверьте порт/фаервол.",
    TimeoutError: "Время ожидания операции истекло. Попробуйте позже.",
    FileNotFoundError: "Файл не найден. Проверьте путь и попробуйте снова.",
    PermissionError: "Нет прав доступа к файлу/папке. Запустите с достаточными правами.",
    ValueError: "Некорректные входные данные. Исправьте и запустите снова.",
}


def to_user_message(exc: BaseException) -> str:
    """Return a short message that is safe to show to end users."""

    for etype, msg in _MAP.items():
        if isinstance(exc, etype):
            return msg
    if isinstance(exc, UserError):
        text = str(exc).strip()
        return text or "Ошибка выполнения операции."
    return "Произошла ошибка. Подробности записаны в лог."
