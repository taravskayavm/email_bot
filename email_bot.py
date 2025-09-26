# Точка входа как раньше:  python email_bot.py
# Ничего не меняет в логике/UI — лишь проксирует вызов в основной main().
from importlib import import_module

_CANDIDATE_MODULES = [
    ("emailbot.messaging_utils", ("main", "run", "start")),
    ("emailbot.messaging",      ("main", "run", "start")),
    ("emailbot.bot.__main__",   ("main", "run", "start")),  # на случай если main расположен здесь
]


def _resolve_entrypoint():
    errors = []
    for mod_name, names in _CANDIDATE_MODULES:
        try:
            mod = import_module(mod_name)
            for attr in names:
                fn = getattr(mod, attr, None)
                if callable(fn):
                    return fn
        except Exception as e:
            errors.append(f"{mod_name}: {e}")
            continue
    raise SystemExit(
        "Не найден main()/run()/start() в известных модулях.\n"
        "Проверьте, где у вас фактический вход, и при необходимости добавьте его в список _CANDIDATE_MODULES."
    )


def main():
    entry = _resolve_entrypoint()
    return entry()


if __name__ == "__main__":
    main()
