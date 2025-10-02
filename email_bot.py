"""Entrypoint: запускает Telegram-бота (на python-telegram-bot, без aiogram)."""
from emailbot.bot.__main__ import main as bot_main


def main() -> int:
    bot_main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
