"""Run callables with a hard timeout, preferring multiprocessing with safe fallbacks."""

from __future__ import annotations

import logging
import multiprocessing as mp
import queue
import threading
import time
import traceback
from multiprocessing.synchronize import Event as _Event
from typing import Any, Callable, Tuple

from emailbot.cancel_token import get_shared_event, install_shared_event, is_cancelled

__all__ = ["run_with_timeout"]


_log = logging.getLogger("emailbot.timeout")


def _proc_target(
    func: Callable[..., Any],
    args: Tuple[Any, ...],
    kwargs: dict[str, Any],
    q: mp.Queue,
    event: _Event,
) -> None:
    try:
        install_shared_event(event)
    except Exception:  # pragma: no cover - defensive best effort
        pass
    try:
        res = func(*args, **kwargs)
        q.put(("ok", res))
    except Exception as exc:  # pragma: no cover - propagate traceback
        q.put(("err", (repr(exc), traceback.format_exc())))


def _thread_target(
    func: Callable[..., Any],
    args: Tuple[Any, ...],
    kwargs: dict[str, Any],
    q: queue.Queue,
    event: _Event,
) -> None:
    try:
        install_shared_event(event)
    except Exception:  # pragma: no cover - defensive best effort
        pass
    try:
        res = func(*args, **kwargs)
        q.put(("ok", res))
    except Exception as exc:  # pragma: no cover - propagate traceback
        q.put(("err", (repr(exc), traceback.format_exc())))


def run_with_timeout(func: Callable[..., Any], timeout_sec: float, *args: Any, **kwargs: Any):
    """Execute ``func`` enforcing ``timeout_sec`` seconds.

    The preferred strategy launches a short-lived ``spawn`` process. If the platform
    or runtime prevents this (e.g. frozen executables or limited environments), the
    function gracefully falls back to a background thread.
    """

    if is_cancelled():
        return None

    if timeout_sec is None or timeout_sec <= 0:
        _log.debug("run_with_timeout: no timeout requested, running inline")
        return func(*args, **kwargs)

    shared_event = get_shared_event()

    try:
        ctx = mp.get_context("spawn")
        q: mp.Queue = ctx.Queue()
        proc = ctx.Process(
            target=_proc_target,
            args=(func, args, kwargs, q, shared_event),
            daemon=True,
            name="run_with_timeout-worker",
        )
        proc.start()
        _log.debug("run_with_timeout: process started pid=%s", getattr(proc, "pid", None))

        # Allow the child to properly start; bail to threads if it never goes alive.
        t0 = time.time()
        started = False
        while time.time() - t0 < 1.5:
            if proc.pid is not None and proc.is_alive():
                started = True
                break
            time.sleep(0.05)
        if not started:
            _log.warning("run_with_timeout: process failed to start, switching to thread fallback")
            try:
                if proc.is_alive():
                    proc.terminate()
                    proc.join(0.5)
            except Exception:  # pragma: no cover - defensive cleanup
                pass
            raise RuntimeError("proc_not_started")

        proc.join(timeout=timeout_sec)
        if proc.is_alive():
            _log.warning(
                "run_with_timeout: timeout %.2fs, terminating process pid=%s",
                timeout_sec,
                proc.pid,
            )
            proc.terminate()
            proc.join(1.0)
            raise TimeoutError(f"run_with_timeout: exceeded {timeout_sec}s")

        try:
            tag, payload = q.get_nowait()
        except queue.Empty:
            _log.error("run_with_timeout: empty result from process")
            raise RuntimeError("run_with_timeout: empty result queue")

        if tag == "ok":
            return payload
        err_repr, tb = payload
        _log.error("run_with_timeout: child error: %s\n%s", err_repr, tb)
        raise RuntimeError(f"run_with_timeout child error: {err_repr}\n{tb}")

    except TimeoutError:
        raise
    except Exception as exc:
        _log.warning("run_with_timeout: switching to thread fallback due to: %r", exc)

        tq: queue.Queue = queue.Queue()
        th = threading.Thread(
            target=_thread_target,
            args=(func, args, kwargs, tq, shared_event),
            daemon=True,
            name="run_with_timeout-thread",
        )
        th.start()
        th.join(timeout=timeout_sec)
        if th.is_alive():
            _log.warning(
                "run_with_timeout(thread): timeout %.2fs, thread did not finish", timeout_sec
            )
            raise TimeoutError(f"run_with_timeout(thread): exceeded {timeout_sec}s")
        try:
            tag, payload = tq.get_nowait()
        except queue.Empty:
            _log.error("run_with_timeout(thread): empty result queue")
            raise RuntimeError("run_with_timeout(thread): empty result queue")
        if tag == "ok":
            return payload
        err_repr, tb = payload
        _log.error("run_with_timeout(thread): child error: %s\n%s", err_repr, tb)
        raise RuntimeError(f"run_with_timeout(thread) error: {err_repr}\n{tb}")

