"""Helper utilities for running archive parsing in a separate process."""

from __future__ import annotations

from typing import Any, Callable, Dict, Tuple
import logging
import multiprocessing as mp
import time
import traceback

from . import extraction as _extraction
from .progress_watchdog import ProgressTracker


logger = logging.getLogger(__name__)


def _worker(zip_path: str, conn) -> None:
    """Worker process entry point."""

    def _emit_progress(snapshot: Dict[str, Any]) -> None:
        try:
            conn.send({"progress": dict(snapshot)})
        except Exception:  # pragma: no cover - best effort delivery
            pass

    tracker = ProgressTracker(on_update=_emit_progress)

    try:
        emails, stats = _extraction.extract_any(zip_path, tracker=tracker)
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


def run_parse_in_subprocess(
    zip_path: str,
    timeout_sec: int,
    *,
    progress_callback: Callable[[Dict[str, Any]], None] | None = None,
) -> Tuple[bool, Dict[str, Any]]:
    """Run ZIP parsing in a forked process with a hard timeout (Windows-safe)."""

    ctx = mp.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    process = ctx.Process(target=_worker, args=(zip_path, child_conn), daemon=False)

    def _notify_progress(payload: Dict[str, Any]) -> None:
        if progress_callback is None:
            return
        try:
            progress_callback(dict(payload))
        except Exception:  # pragma: no cover - defensive
            logger.debug("progress callback failed", exc_info=True)

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

    deadline = time.monotonic() + timeout_sec
    data: Dict[str, Any] | None = None

    def _recv_message() -> Dict[str, Any] | None:
        try:
            message = parent_conn.recv()
        except (EOFError, OSError):  # pragma: no cover - defensive
            return None
        if isinstance(message, dict) and "progress" in message:
            payload = message.get("progress")
            if isinstance(payload, dict):
                _notify_progress(payload)
            return None
        return message

    while time.monotonic() < deadline:
        try:
            if parent_conn.poll(0.2):
                message = _recv_message()
                if message is not None:
                    data = message
                    break
        except (EOFError, OSError):  # pragma: no cover - defensive
            break

        if not process.is_alive():
            drained = False
            try:
                while parent_conn.poll(0.1):
                    message = _recv_message()
                    if message is not None:
                        data = message
                        drained = True
                        break
            except Exception:  # pragma: no cover - defensive
                data = None
            if drained or data is not None:
                break

    if data is None:
        timed_out = time.monotonic() >= deadline
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

    stats = data.get("stats") or {}
    if isinstance(stats, dict):
        _notify_progress(stats)

    return True, {"emails": data["emails"], "stats": stats}


__all__ = ["run_parse_in_subprocess"]
