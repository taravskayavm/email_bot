"""Utilities for enforcing hard timeouts when running callables."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import functools
from typing import Any, Callable

__all__ = ["DEFAULT_TIMEOUT_SEC", "run_with_timeout"]


# Жёсткий таймаут по умолчанию на одну операцию (сек)
DEFAULT_TIMEOUT_SEC = 20


def run_with_timeout(
    func: Callable[..., Any], timeout_sec: int = DEFAULT_TIMEOUT_SEC, *args: Any, **kwargs: Any
) -> Any:
    """Execute ``func`` in a short-lived thread pool and enforce ``timeout_sec`` seconds."""

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(functools.partial(func, *args, **kwargs))
        try:
            return future.result(timeout=timeout_sec)
        except FuturesTimeoutError as exc:  # pragma: no cover - exercised via integration tests
            future.cancel()
            raise TimeoutError(f"Operation timed out after {timeout_sec}s") from exc
