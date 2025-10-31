"""Helper utilities for running archive parsing in a separate process."""

from __future__ import annotations

from multiprocessing import Pipe, Process
from typing import Any, Dict, Tuple
import logging
import traceback

from . import extraction as _extraction


logger = logging.getLogger(__name__)


def _worker(zip_path: str, conn) -> None:
    """Worker process entry point."""

    try:
        emails, stats = _extraction.extract_any(zip_path)
        conn.send({"ok": True, "emails": emails, "stats": stats})
    except Exception as exc:  # pragma: no cover - defensive fallback
        tb = traceback.format_exc()
        logger.warning("zip worker failed: %s", exc)
        conn.send({"ok": False, "error": str(exc), "traceback": tb})
    finally:
        try:
            conn.close()
        except Exception:  # pragma: no cover - best effort cleanup
            pass


def run_parse_in_subprocess(zip_path: str, timeout_sec: int) -> Tuple[bool, Dict[str, Any]]:
    """Run ZIP parsing in a forked process with a hard timeout."""

    parent_conn, child_conn = Pipe(duplex=False)
    process = Process(target=_worker, args=(zip_path, child_conn), daemon=True)
    process.start()
    child_conn.close()

    process.join(timeout=timeout_sec)
    if process.is_alive():
        try:
            process.terminate()
        except Exception:  # pragma: no cover - defensive
            pass
        process.join(2)
        return False, {"error": f"timeout after {timeout_sec}s"}

    try:
        if parent_conn.poll(0.1):
            data = parent_conn.recv()
        else:
            return False, {"error": "no result from subprocess"}
    finally:
        try:
            parent_conn.close()
        except Exception:  # pragma: no cover - best effort cleanup
            pass

    if not data.get("ok"):
        return False, {
            "error": data.get("error", "unknown error"),
            "traceback": data.get("traceback"),
        }

    return True, {"emails": data["emails"], "stats": data.get("stats", {})}


__all__ = ["run_parse_in_subprocess"]
