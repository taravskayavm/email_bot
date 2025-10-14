"""Simple smoke-test for the mass send executor without hitting SMTP."""

from __future__ import annotations

import sys
import traceback


def main() -> int:
    try:
        from emailbot.handlers import manual_send as ms  # type: ignore
    except Exception as exc:  # pragma: no cover - diagnostic path
        sys.stderr.write(f"[smoke] import manual_send failed: {exc!r}\n")
        return 1

    try:
        make_pool = getattr(ms, "_make_pool", None)
        if not callable(make_pool):
            sys.stderr.write("[smoke] _make_pool not callable\n")
            return 1
        with make_pool() as executor:
            futures = [executor.submit(lambda x=i: x, i) for i in (1, 2)]
            values = [future.result(timeout=5) for future in futures]
            if values != [1, 2]:
                sys.stderr.write(f"[smoke] unexpected executor results: {values!r}\n")
                return 1
    except Exception:  # pragma: no cover - smoke diagnostics
        traceback.print_exc()
        return 1

    print("[smoke] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
