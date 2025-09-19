#!/usr/bin/env python
"""Rotate the send stats log when it grows too large."""
# [EBOT-LOG-ROTATE-006]
import gzip
import os
import shutil
import time
from pathlib import Path

from utils.paths import expand_path

PATH = expand_path(os.getenv("SEND_STATS_PATH", "var/send_stats.jsonl"))
MAX_SIZE_MB = int(os.getenv("SEND_STATS_MAX_MB", "50"))


def main() -> None:
    """Rotate the send stats file if it exceeds the configured size."""
    if not PATH.exists():
        print("no stats file")
        return

    size_mb = PATH.stat().st_size / (1024 * 1024)
    if size_mb < MAX_SIZE_MB:
        print(f"ok ({size_mb:.1f}MB < {MAX_SIZE_MB}MB)")
        return

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    destination = PATH.with_name(f"{PATH.name}.{timestamp}.gz")

    with open(PATH, "rb") as src, gzip.open(destination, "wb", compresslevel=6) as gz:
        shutil.copyfileobj(src, gz)

    PATH.write_text("")
    print(f"rotated -> {destination}")


if __name__ == "__main__":
    main()
