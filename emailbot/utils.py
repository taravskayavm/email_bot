import logging
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv

logger = logging.getLogger(__name__)
_CHECKED_ENV_PATHS: set[Path] = set()


def _warn_duplicate_keys(path: Path) -> None:
    """Log duplicate keys found in the provided ``.env`` file."""

    try:
        resolved = path.resolve()
    except Exception:
        resolved = path
    if resolved in _CHECKED_ENV_PATHS:
        return
    _CHECKED_ENV_PATHS.add(resolved)
    try:
        if not path.exists():
            return
        keys: list[str] = []
        with path.open("r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.lower().startswith("export "):
                    line = line[7:].lstrip()
                key, sep, _ = line.partition("=")
                if not sep:
                    key, sep, _ = line.partition(":")
                key = key.strip()
                if not key:
                    continue
                keys.append(key)
        duplicates = [key for key, count in Counter(keys).items() if count > 1]
        for dup in duplicates:
            logger.warning("Duplicate .env key: %s (using last)", dup)
    except Exception as exc:
        logger.debug("duplicate .env check failed for %s: %r", path, exc)


def warn_duplicate_env_keys(path: Path) -> None:
    """Public helper to warn about duplicate keys in ``path``."""

    _warn_duplicate_keys(path)


def _warn_duplicates(paths: Iterable[Path]) -> None:
    for candidate in paths:
        try:
            _warn_duplicate_keys(candidate)
        except Exception:
            logger.debug("duplicate .env check raised", exc_info=True)


def load_env(script_dir: Path) -> None:
    """Load environment variables from .env files."""

    try:
        primary = script_dir / ".env"
        load_dotenv(dotenv_path=primary)
        _warn_duplicates([primary])
        load_dotenv()
        _warn_duplicates([Path(".env")])
    except Exception as exc:
        logger.debug("load_env failed: %r", exc)


def setup_logging(log_file: Path) -> None:
    """Configure basic logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


def log_error(msg: str) -> None:
    """Log an error message and append it to ``bot_errors.log``."""
    logger.error(msg)
    try:
        err_file = Path(__file__).resolve().parent / "bot_errors.log"
        with err_file.open("a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()} {msg}\n")
    except Exception as exc:
        logger.debug("log_error append failed: %r", exc)


try:  # pragma: no cover - optional bridge for legacy imports
    from . import utils_preview_export as _preview_export

    sys.modules[__name__ + ".preview_export"] = _preview_export
except Exception as exc:  # pragma: no cover - ignore if optional dependency missing
    logger.debug("preview_export bridge not available: %r", exc)
