"""Module entry-point to launch the email bot via ``python -m emailbot``."""

from __future__ import annotations

import runpy
import sys

if __name__ == "__main__":
    try:
        runpy.run_module("email_bot", run_name="__main__")
    except SystemExit:
        raise
    except Exception as exc:  # pragma: no cover - defensive startup guard
        sys.stderr.write(f"[emailbot.__main__] fatal: {exc!r}\n")
        raise
