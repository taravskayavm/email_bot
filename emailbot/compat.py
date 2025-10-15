"""Compatibility warm-up hooks."""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def apply() -> None:
    """Warm up dynamic compatibility shims."""

    try:
        from . import settings as _s

        _ = (
            _s.SEND_MAX_WORKERS,
            _s.SEND_FILE_TIMEOUT,
            _s.SEND_COOLDOWN_DAYS,
            _s.PARSE_MAX_WORKERS,
            _s.PARSE_FILE_TIMEOUT,
        )
    except Exception as exc:
        log.debug("compat.apply: settings warmup failed: %r", exc)

    try:
        from . import messaging as _m

        _ = (
            _m._normalize_key,
            _m._should_skip_by_history,
            _m.run_in_app_loop,
        )
    except Exception as exc:
        log.debug("compat.apply: messaging warmup failed: %r", exc)
