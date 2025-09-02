"""Runtime configurable settings for the bot."""

from __future__ import annotations

import json
from pathlib import Path

SETTINGS_PATH = Path("/mnt/data/settings.json")

# Default values
STRICT_OBFUSCATION: bool = True
FOOTNOTE_RADIUS_PAGES: int = 1


def load() -> None:
    """Load configuration from :data:`SETTINGS_PATH`."""

    global STRICT_OBFUSCATION, FOOTNOTE_RADIUS_PAGES
    try:
        data = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    STRICT_OBFUSCATION = bool(data.get("STRICT_OBFUSCATION", STRICT_OBFUSCATION))
    FOOTNOTE_RADIUS_PAGES = int(data.get("FOOTNOTE_RADIUS_PAGES", FOOTNOTE_RADIUS_PAGES))


def save() -> None:
    """Persist current configuration."""

    try:
        SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with SETTINGS_PATH.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "STRICT_OBFUSCATION": STRICT_OBFUSCATION,
                    "FOOTNOTE_RADIUS_PAGES": FOOTNOTE_RADIUS_PAGES,
                },
                f,
            )
    except Exception:
        pass


# Load settings on module import.
load()


__all__ = ["STRICT_OBFUSCATION", "FOOTNOTE_RADIUS_PAGES", "load", "save"]

