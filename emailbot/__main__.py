# Позволяет запускать: python -m email_bot
from pathlib import Path
import runpy
import sys

def _run_legacy_entry():
    # Запускаем корневой email_bot.py как скрипт — это сохраняет «старую» семантику.
    root = Path(__file__).resolve().parent.parent / "email_bot.py"
    if not root.exists():
        raise SystemExit("email_bot.py не найден в корне репозитория.")
    runpy.run_path(str(root), run_name="__main__")

if __name__ == "__main__":
    _run_legacy_entry()
