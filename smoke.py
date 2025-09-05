#!/usr/bin/env python3
"""Run extractor on gold fixtures and print a short summary."""

from __future__ import annotations

import pathlib

from emailbot.extraction import strip_html, smart_extract_emails, extract_from_pdf


def main() -> None:
    base = pathlib.Path("tests/fixtures/gold")
    for path in sorted(base.iterdir()):
        if path.suffix == ".pdf":
            hits, stats = extract_from_pdf(str(path))
            count = len(hits)
            q = stats.get("quarantined", 0)
        else:
            text = strip_html(path.read_text(encoding="utf-8"))
            stats: dict = {}
            emails = smart_extract_emails(text, stats)
            count = len(emails)
            q = stats.get("quarantined", 0)
        print(f"{path.name}: {count} ok, {q} quarantined")


if __name__ == "__main__":
    main()
