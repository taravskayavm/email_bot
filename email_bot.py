# === EBOT launcher (works from repo root or subfolder) ===
# - Finds the repository root and adds it to sys.path
# - Loads .env (from CWD or repo root) if python-dotenv is installed
# - Composes candidate modules to look for entrypoints: main/run/start
# - Allows overriding candidates via CANDIDATES env var
# - Then resolves and calls the first available entrypoint

import os
import sys
from importlib import import_module
from typing import Iterable, Tuple


# ---------- helpers: repo root & dotenv ----------

def _find_repo_root(start: str, max_up: int = 6) -> str | None:
    """
    Walk up to `max_up` levels looking for a directory that contains
    both '.git' and 'requirements.txt' — treat it as the repo root.
    """
    cur = os.path.abspath(start)
    for _ in range(max_up):
        git_dir = os.path.join(cur, ".git")
        req_file = os.path.join(cur, "requirements.txt")
        if os.path.isdir(git_dir) and os.path.isfile(req_file):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
    return None


# Add repo root to sys.path if we can detect it (supports running from a subfolder)
_REPO_ROOT = _find_repo_root(os.getcwd())
if _REPO_ROOT and _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# Try to load .env (from current dir or repo root), if python-dotenv is available
try:
    from dotenv import load_dotenv, find_dotenv  # type: ignore
    _env_path = find_dotenv(usecwd=True) or os.path.join(_REPO_ROOT or os.getcwd(), ".env")
    if _env_path and os.path.isfile(_env_path):
        load_dotenv(_env_path)
        print(f"[ebot] .env loaded from: {_env_path}")
except Exception:
    # dotenv is optional; environment variables may already be present
    pass


# ---------- candidates configuration ----------

DEFAULT_ENTRY_NAMES: Tuple[str, ...] = ("main", "run", "start")

def _compose_candidates() -> Iterable[Tuple[str, Tuple[str, ...]]]:
    """
    Compose a list of (module_name, entry_names) to probe for an entrypoint.
    Order:
      1) Modules listed in env var CANDIDATES (comma-separated),
      2) Project defaults (with and without 'emailbot.' prefix).
    """
    env_candidates: Iterable[str] = ()
    env_val = os.getenv("CANDIDATES", "").strip()
    if env_val:
        env_candidates = [m.strip() for m in env_val.split(",") if m.strip()]

    defaults = [
        # canonical project modules
        "emailbot.messaging_utils",
        "emailbot.messaging",
        "emailbot.bot.__main__",
        # fallbacks without prefix (for historical subfolder launches)
        "bot.__main__",
        "messaging",
        "bot",
    ]

    seen = set()
    ordered: list[Tuple[str, Tuple[str, ...]]] = []
    for name in list(env_candidates) + defaults:
        if name not in seen:
            seen.add(name)
            ordered.append((name, DEFAULT_ENTRY_NAMES))
    return ordered


CANDIDATES: Iterable[Tuple[str, Tuple[str, ...]]] = _compose_candidates()


# ---------- entrypoint resolution ----------

def resolve_entrypoint():
    """
    Try importing each candidate module and look for a callable named
    'main' or 'run' or 'start'. Return the first one found.
    """
    for mod_name, names in CANDIDATES:
        try:
            mod = import_module(mod_name)
        except Exception:
            continue
        for name in names:
            fn = getattr(mod, name, None)
            if callable(fn):
                return fn
    raise SystemExit(
        "Не найдено ни одной точки входа (main/run/start). "
        "Уточните, в каком модуле она находится, и задайте переменную окружения CANDIDATES, "
        "например: CANDIDATES=emailbot.messaging,emailbot.messaging_utils"
    )


def main():
    entry = resolve_entrypoint()
    return entry()


if __name__ == "__main__":
    main()