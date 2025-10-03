import importlib
import os
import sys
from pathlib import Path

def _load_dotenv_safe():
    """
    Ленивая подгрузка .env, чтобы поведение оставалось как раньше:
    при наличии .env в корне — загрузить и написать, откуда загрузили.
    Не добавляем зависимостей; если python-dotenv не установлен — просто пропускаем.
    """
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_path)
            print(f"[ebot] .env loaded from: {env_path}")
        except Exception:
            # Безопасно замалчиваем — старое поведение было «мягким»
            print(f"[ebot] .env present but python-dotenv not available (skipped): {env_path}")

def _try_call(func):
    if callable(func):
        return func()
    return None

def _resolve_and_run():
    """
    Универсальный «старый» запуск:
    пытаемся найти в известных модулях одну из функций: main / run / start / cli
    и вызвать её без аргументов.
    Можно переопределить через CLI:
      python email_bot.py --module emailbot.messaging --func main
    """
    # Быстрый ручной оверрайд через флаги
    mod_override = None
    func_override = None
    if "--module" in sys.argv:
        i = sys.argv.index("--module")
        if i + 1 < len(sys.argv):
            mod_override = sys.argv[i+1]
    if "--func" in sys.argv:
        i = sys.argv.index("--func")
        if i + 1 < len(sys.argv):
            func_override = sys.argv[i+1]

    # Набор вероятных модулей из проекта (упорядочены по приоритету)
    candidates_modules = [
        "emailbot.app",
        "emailbot.bot",
        "emailbot.messaging",
        "emailbot.runner",
        "emailbot.main",
        "emailbot.core",
        "emailbot.cli",
    ]
    # Наиболее типовые имена точек входа
    candidate_funcs = ["main", "run", "start", "cli"]

    if mod_override:
        modules = [mod_override]
    else:
        modules = candidates_modules

    if func_override:
        funcs = [func_override]
    else:
        funcs = candidate_funcs

    last_err = None
    for m in modules:
        try:
            mod = importlib.import_module(m)
        except Exception as e:
            last_err = e
            continue
        for f in funcs:
            fn = getattr(mod, f, None)
            if callable(fn):
                return _try_call(fn)
    # Если ничего не нашли — даем понятное сообщение (как раньше, но без CANDIDATES)
    raise RuntimeError(
        "Не найдена точка входа. "
        "Проверьте, что в одном из модулей пакета emailbot есть функция main/run/start/cli.\n"
        "Можно указать явно: python email_bot.py --module emailbot.X --func main"
        + (f"\nПоследняя ошибка импорта: {last_err}" if last_err else "")
    )

def main():
    _load_dotenv_safe()
    # На всякий случай гасим влияние новой переменной, если она осталась в окружении
    os.environ.pop("CANDIDATES", None)
    return _resolve_and_run()

if __name__ == "__main__":
    sys.exit(main() or 0)
