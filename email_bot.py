from __future__ import annotations

import asyncio
import inspect
from importlib import import_module


# Порядок важен: сначала старые точки входа, затем новые.
CANDIDATES = [
    ("emailbot.messaging_utils", ("main", "run", "start")),
    ("emailbot.messaging", ("main", "run", "start")),
    ("emailbot.bot.__main__", ("main", "run", "start")),
]


def resolve_entrypoint():
    for mod_name, names in CANDIDATES:
        try:
            mod = import_module(mod_name)
            for name in names:
                fn = getattr(mod, name, None)
                if callable(fn):
                    return fn
        except Exception:
            # Переходим к следующему варианту
            pass
    raise SystemExit(
        "Не найдено ни одной точки входа (main/run/start). "
        "Уточните, в каком модуле она находится, и добавьте его в CANDIDATES."
    )


def _run_entrypoint(entrypoint):
    """Запускает синхронную или асинхронную точку входа."""

    def _ensure_windows_policy():
        try:
            import sys

            if sys.platform.startswith("win"):
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            # Если не получилось выставить политику — продолжаем без ошибок
            pass

    if inspect.iscoroutinefunction(entrypoint):
        _ensure_windows_policy()
        return asyncio.run(entrypoint())

    result = entrypoint()
    if inspect.isawaitable(result):
        _ensure_windows_policy()
        return asyncio.run(result)

    return result


def main():
    return _run_entrypoint(resolve_entrypoint())


if __name__ == "__main__":
    main()
