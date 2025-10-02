"""Entrypoint: запускает Telegram-бота на PTB (без aiogram)."""
from emailbot.bot.__main__ import main_sync as entrypoint


if __name__ == "__main__":
    entrypoint()
