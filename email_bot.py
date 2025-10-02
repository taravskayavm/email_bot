"""Synchronous entrypoint resolver for the email bot project."""

from importlib import import_module
import sys

# Порядок важен: сначала старые точки входа, затем новые.
CANDIDATES = [
    ("emailbot.messaging_utils", ("main", "run", "start")),
    ("emailbot.messaging",      ("main", "run", "start")),
    # ВАЖНО: бот больше не является резервной точкой входа для CLI.
    # Чтобы запустить бота, используйте:
    #   python -m emailbot.bot
]


def resolve_entrypoint():
    for mod_name, names in CANDIDATES:
        try:
            mod = import_module(mod_name)
            for name in names:
                fn = getattr(mod, name, None)
                if fn is None:
                    continue
                # Нашли подходящую функцию — возвращаем её
                if callable(fn):
                    return fn
        except Exception:
            # Переходим к следующему варианту
            pass
    sys.stderr.write(
        "CLI-точка входа не найдена (main/run/start) ни в emailbot.messaging_utils, ни в emailbot.messaging.\n"
        "Бот по умолчанию больше не запускается из email_bot.py.\n"
        "Если нужен Telegram-бот — запустите отдельно:  python -m emailbot.bot\n"
    )
    raise SystemExit(2)


def main():
    """Invoke the first available entrypoint synchronously."""

    return resolve_entrypoint()()


if __name__ == "__main__":
    main()
