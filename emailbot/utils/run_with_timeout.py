"""Process-based timeout helpers with Windows-friendly spawn support."""

from __future__ import annotations

import logging
import multiprocessing as mp
import queue
import sys
import threading
import time
import traceback
from typing import Any, Callable

from multiprocessing.synchronize import Event as _Event

from emailbot.cancel_token import (
    get_shared_event,
    install_shared_event,
    is_cancelled,
)

__all__ = ["run_with_timeout"]


logger = logging.getLogger(__name__)


def _ensure_spawn_context() -> mp.context.BaseContext:
    """Return a safe multiprocessing context (spawn on Windows)."""

    if sys.platform.startswith("win"):
        try:
            mp.set_start_method("spawn", force=True)
        except RuntimeError:
            # already set by the parent process
            pass
        return mp.get_context("spawn")
    try:
        return mp.get_context()
    except ValueError:
        return mp.get_context("spawn")


def _run_in_thread(
    func: Callable[..., Any],
    timeout: int | float | None,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    shared_event: _Event,
) -> Any:
    """Execute ``func`` in a background thread when process spawn fails."""

    result: dict[str, Any] = {}
    done = threading.Event()

    def _worker() -> None:
        try:
            install_shared_event(shared_event)
        except Exception:  # pragma: no cover - best effort
            pass
        try:
            result["value"] = func(*args, **kwargs)
        except BaseException as exc:  # pragma: no cover - propagate caller error
            result["error"] = exc
            result["traceback"] = traceback.format_exc()
        finally:
            done.set()

    thread = threading.Thread(target=_worker, name="run_with_timeout-fallback", daemon=True)
    thread.start()

    if timeout is None or timeout <= 0:
        timeout = None
    finished = done.wait(timeout)
    if not finished:
        logger.warning("run_with_timeout fallback thread timed out after %ss", timeout)
        raise TimeoutError(f"Timeout after {timeout}s")

    if "error" in result:
        exc = result["error"]
        tb_text = result.get("traceback")
        if tb_text:
            logger.debug("fallback thread exception:\n%s", tb_text)
        raise exc

    return result.get("value")


def run_with_timeout(func: Callable[..., Any], timeout: int, *args: Any, **kwargs: Any):
    """Execute ``func`` in an isolated worker with a hard timeout."""

    if is_cancelled():
        return None

    ctx = _ensure_spawn_context()
    shared_event = get_shared_event()
    q = ctx.Queue()

    def _target(queue_: mp.queues.Queue, event: _Event) -> None:
        try:
            install_shared_event(event)
        except Exception:  # pragma: no cover - best effort
            pass
        try:
            value = func(*args, **kwargs)
        except BaseException as exc:  # pragma: no cover - propagate caller error
            queue_.put(("error", repr(exc), traceback.format_exc()))
        else:
            queue_.put(("ok", value, None))

    try:
        proc = ctx.Process(
            target=_target,
            args=(q, shared_event),
            name="run_with_timeout-worker",
            daemon=True,
        )
        proc.start()
    except Exception as exc:
        logger.warning(
            "run_with_timeout failed to spawn process, switching to thread fallback", exc_info=True
        )
        if isinstance(q, mp.queues.Queue):
            try:
                q.close()
                q.join_thread()
            except Exception:  # pragma: no cover - cleanup best effort
                pass
        return _run_in_thread(func, timeout, args, kwargs, shared_event)

    deadline = None if timeout is None else time.monotonic() + float(timeout)
    message: tuple[str, Any, Any] | None = None

    try:
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                break
            remaining = None
            if deadline is not None:
                remaining = max(0.0, deadline - time.monotonic())
            wait_for = min(0.2, remaining) if remaining is not None else 0.2
            try:
                message = q.get(timeout=wait_for)
                break
            except queue.Empty:
                pass
            if not proc.is_alive():
                try:
                    message = q.get_nowait()
                except queue.Empty:
                    message = None
                break

        if deadline is not None and time.monotonic() >= deadline and message is None:
            proc.terminate()
            proc.join(1)
            raise TimeoutError(f"Timeout after {timeout}s")

        if message is None:
            if proc.exitcode not in (0, None):
                raise RuntimeError(f"Worker exited with code {proc.exitcode}")
            raise RuntimeError("Worker finished without returning a result")

        status, payload, tb_text = message
        if status == "ok":
            return payload
        if status == "error":
            if tb_text:
                logger.debug("worker exception:\n%s", tb_text)
            raise RuntimeError(str(payload))
        raise RuntimeError(f"Unknown worker status: {status}")
    finally:
        try:
            proc.join(0.1)
        except Exception:
            pass
        if proc.is_alive():
            try:
                proc.terminate()
            except Exception:  # pragma: no cover - defensive
                pass
            proc.join(1)
        try:
            q.close()
            q.join_thread()
        except Exception:  # pragma: no cover - cleanup best effort
            pass
