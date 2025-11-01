"""Helper utilities for running archive parsing in a separate process."""

from __future__ import annotations

from typing import Any, Dict, Tuple
import logging
import multiprocessing as mp
import time
import traceback

from . import extraction as _extraction


logger = logging.getLogger(__name__)


def _worker(zip_path: str, conn) -> None:
    """Worker process entry point."""

    try:
        emails, stats = _extraction.extract_any(zip_path)
        try:
            conn.send({"ok": True, "emails": emails, "stats": stats})
        except Exception:  # pragma: no cover - best effort delivery
            pass
    except Exception as exc:  # pragma: no cover - defensive fallback
        tb = traceback.format_exc()
        logger.warning("zip worker failed: %s", exc)
        try:
            conn.send({"ok": False, "error": str(exc), "traceback": tb})
        except Exception:  # pragma: no cover - best effort delivery
            pass
    finally:
        try:
            conn.close()
        except Exception:  # pragma: no cover - best effort cleanup
            pass


def run_parse_in_subprocess(zip_path: str, timeout_sec: int) -> Tuple[bool, Dict[str, Any]]:
    """Run ZIP parsing in a forked process with a hard timeout (Windows-safe)."""

    ctx = mp.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    process = ctx.Process(target=_worker, args=(zip_path, child_conn), daemon=False)

    try:
        process.start()
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("failed to start zip worker: %s", exc)
        try:
            child_conn.close()
        except Exception:  # pragma: no cover - best effort cleanup
            pass
        try:
            parent_conn.close()
        except Exception:  # pragma: no cover - best effort cleanup
            pass
        return False, {"error": f"failed to start subprocess: {exc}"}

    try:
        child_conn.close()
    except Exception:  # pragma: no cover - best effort cleanup
        pass

    deadline = time.time() + timeout_sec
    data: Dict[str, Any] | None = None

    while time.time() < deadline:
        try:
            if parent_conn.poll(0.2):
                try:
                    data = parent_conn.recv()
                except (EOFError, OSError):  # pragma: no cover - defensive
                    data = None
                break
        except (EOFError, OSError):  # pragma: no cover - defensive
            break

        if not process.is_alive():
            try:
                if parent_conn.poll(0.1):
                    data = parent_conn.recv()
            except Exception:  # pragma: no cover - defensive
                data = None
            break

    if data is None:
        timed_out = time.time() >= deadline
        try:
            if process.is_alive():
                process.terminate()
                process.join(2.0)
                if process.is_alive() and hasattr(process, "kill"):
                    process.kill()
        except Exception:  # pragma: no cover - defensive
            pass

        try:
            parent_conn.close()
        except Exception:  # pragma: no cover - best effort cleanup
            pass

        if timed_out:
            return False, {"error": f"timeout after {timeout_sec}s"}

        try:
            process.join(timeout=0.0)
        except Exception:  # pragma: no cover - defensive
            pass
        return False, {"error": "no result from subprocess"}

    try:
        parent_conn.close()
    except Exception:  # pragma: no cover - best effort cleanup
        pass

    if process.is_alive():
        try:
            process.join(0.1)
        except Exception:  # pragma: no cover - defensive
            pass

    if not data.get("ok"):
        return False, {
            "error": data.get("error", "unknown error"),
            "traceback": data.get("traceback"),
        }

    return True, {"emails": data["emails"], "stats": data.get("stats", {})}


__all__ = ["run_parse_in_subprocess"]
