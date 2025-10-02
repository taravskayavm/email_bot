"""Entry point: always start Telegram bot."""
from emailbot.bot.__main__ import main as bot_main


def main() -> int:
    """Запуск синхронной обёртки Telegram-бота."""
    return bot_main() or 0


if __name__ == "__main__":
    raise SystemExit(main())
