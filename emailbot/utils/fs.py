from __future__ import annotations

from pathlib import Path
import json
import os
import tempfile
import time
from typing import Any

try:
    import fcntl  # type: ignore[attr-defined]

    _HAS_FCNTL = True
except Exception:  # pragma: no cover - Windows fallback
    _HAS_FCNTL = False


def _lock_file(f) -> None:
    if _HAS_FCNTL:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)


def _unlock_file(f) -> None:
    if _HAS_FCNTL:
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def append_jsonl_atomic(path: Path, obj: Any, retries: int = 5, delay: float = 0.05) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(obj, ensure_ascii=False) + "\n"
    for _ in range(retries):
        try:
            with open(path, "a", encoding="utf-8") as f:
                _lock_file(f)
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
                _unlock_file(f)
            return
        except Exception:
            time.sleep(delay)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding="utf-8") as tf:
        tf.write(line)
        tf.flush()
        os.fsync(tf.fileno())
        tmp = Path(tf.name)
    with open(path, "a", encoding="utf-8") as f:
        with open(tmp, "r", encoding="utf-8") as rf:
            f.write(rf.read())
    tmp.unlink(missing_ok=True)
