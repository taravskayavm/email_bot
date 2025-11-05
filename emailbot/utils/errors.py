"""Domain-specific exception types for the bot."""


class UserError(Exception):
    """Ошибки, которые безопасно показывать пользователю в интерфейсе."""


class SystemError(Exception):
    """Системные ошибки инфраструктуры (логируем подробно, пользователю — кратко)."""
