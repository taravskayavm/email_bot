from __future__ import annotations

import asyncio
import inspect
import sys

# Импортируем точку входа бота/приложения, как и раньше:
from emailbot.bot.__main__ import main as entrypoint


def _run_entrypoint() -> object:
    """
    Универсальный запуск: если main — корутина, используем asyncio.run(),
    иначе — обычный вызов. Нужно, чтобы не получать
    'coroutine was never awaited' на async main().
    """

    if inspect.iscoroutinefunction(entrypoint):
        # На Windows для PTB/HTTP-клиентов часто нужен SelectorPolicy
        try:
            if sys.platform.startswith("win"):
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass
        return asyncio.run(entrypoint())
    else:
        return entrypoint()


if __name__ == "__main__":
    _run_entrypoint()
