"""Helpers for building lightweight preview files."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable

try:  # pragma: no cover - optional dependency
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pd = None  # type: ignore


def _normalize(emails: Iterable[str]) -> list[str]:
    seen: dict[str, None] = {}
    result: list[str] = []
    for email in emails:
        value = (email or "").strip()
        if not value or value in seen:
            continue
        seen[value] = None
        result.append(value)
    return result


def build_preview_excel(to_send: Iterable[str], suspects: Iterable[str]) -> str:
    """Create a lightweight preview file and return its path as string."""

    outdir = Path("var")
    outdir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path_xlsx = outdir / f"preview_{timestamp}.xlsx"
    path_csv = outdir / f"preview_{timestamp}.csv"

    cleaned_to_send = _normalize(to_send)
    cleaned_suspects = _normalize(suspects)

    if pd is None:
        content = ["to_send", *cleaned_to_send, "", "suspects", *cleaned_suspects]
        path_csv.write_text("\n".join(content), encoding="utf-8")
        return str(path_csv)

    with pd.ExcelWriter(path_xlsx, engine="openpyxl") as writer:  # type: ignore[arg-type]
        pd.DataFrame({"email": cleaned_to_send}).to_excel(
            writer, sheet_name="to_send", index=False
        )
        pd.DataFrame({"email": cleaned_suspects}).to_excel(
            writer, sheet_name="suspects", index=False
        )
    return str(path_xlsx)

