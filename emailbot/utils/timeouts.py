"""Utilities for running callables with a hard timeout."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FuturesTimeoutError
from typing import Any, Callable, Iterable, Tuple


class TimeoutError(Exception):
    """Raised when the wrapped callable exceeds the allotted timeout."""


def run_with_timeout(
    func: Callable[..., Any],
    *,
    args: Iterable[Any] | None = None,
    kwargs: dict[str, Any] | None = None,
    timeout: float,
    max_workers: int = 1,
) -> Tuple[bool, Any]:
    """Execute ``func`` in a thread pool enforcing ``timeout`` seconds.

    The function returns a tuple ``(ok, result)`` where ``ok`` is ``True`` on
    success and ``result`` contains the callable return value. When the
    execution fails (either by timing out or raising), ``ok`` is ``False`` and
    ``result`` contains the exception instance. ``TimeoutError`` is used to
    signal that the execution did not finish within the specified window.
    """

    call_args = tuple(args or ())
    call_kwargs = dict(kwargs or {})

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future = executor.submit(func, *call_args, **call_kwargs)
        try:
            return True, future.result(timeout=timeout)
        except _FuturesTimeoutError:
            return False, TimeoutError(f"Timed out after {timeout}s")
        except Exception as exc:  # pragma: no cover - passthrough
            return False, exc
