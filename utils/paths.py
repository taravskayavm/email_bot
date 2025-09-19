from __future__ import annotations

import os
import tempfile
from pathlib import Path


def expand_path(p: str | os.PathLike) -> Path:
    """Expand environment variables and ``~``, return absolute :class:`Path`."""

    s = os.fspath(p)
    s = os.path.expandvars(os.path.expanduser(s))
    return Path(s).resolve()


def ensure_parent(path: Path) -> None:
    """Ensure parent directory for ``path`` exists."""

    path.parent.mkdir(parents=True, exist_ok=True)


def get_temp_dir(prefix: str = "emailbot") -> Path:
    """Return process-wide temp directory located under system temp root."""

    base = Path(tempfile.gettempdir()).resolve()
    d = base / prefix
    d.mkdir(parents=True, exist_ok=True)
    return d


def get_temp_file(name: str, prefix: str = "emailbot") -> Path:
    """Return path to named file inside :func:`get_temp_dir`."""

    return get_temp_dir(prefix) / name
