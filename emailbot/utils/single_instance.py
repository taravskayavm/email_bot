from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

_LOCK_DIR = Path(os.getenv("RUNTIME_DIR", "var"))
_LOCK_DIR.mkdir(parents=True, exist_ok=True)


def _posix_lock(fd: int) -> None:
    import fcntl  # type: ignore[attr-defined]

    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _posix_unlock(fd: int) -> None:
    import fcntl  # type: ignore[attr-defined]

    fcntl.flock(fd, fcntl.LOCK_UN)


def _win_lock(fh) -> None:
    import msvcrt  # type: ignore[attr-defined]

    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)


def _win_unlock(fh) -> None:
    import msvcrt  # type: ignore[attr-defined]

    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)


@contextmanager
def single_instance_lock(name: str = "bot") -> Iterator[None]:
    """Create ``var/<name>.lock`` and hold exclusive lock; raise if busy."""

    lock_path = _LOCK_DIR / f"{name}.lock"
    if os.name == "nt":
        fh = open(lock_path, "a+", encoding="utf-8")
        try:
            _win_lock(fh)
        except Exception as exc:  # pragma: no cover - platform specific
            fh.close()
            raise RuntimeError(f"lock-busy:{lock_path}") from exc
        try:
            fh.seek(0)
            fh.truncate(0)
            fh.write(str(os.getpid()))
            fh.flush()
            yield
        finally:
            try:
                _win_unlock(fh)
            finally:
                fh.close()
    else:
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            _posix_lock(fd)
        except Exception as exc:  # pragma: no cover - platform specific
            os.close(fd)
            raise RuntimeError(f"lock-busy:{lock_path}") from exc
        try:
            os.ftruncate(fd, 0)
            os.write(fd, str(os.getpid()).encode())
            os.fsync(fd)
            yield
        finally:
            try:
                _posix_unlock(fd)
            finally:
                os.close(fd)
