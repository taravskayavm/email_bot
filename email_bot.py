"""Синхронная точка входа (без asyncio)."""
from emailbot.bot.__main__ import main_sync as entrypoint


if __name__ == "__main__":
    entrypoint()
