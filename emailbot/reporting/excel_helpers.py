"""Helpers for enriching Excel previews with additional sheets."""

from __future__ import annotations

import os
from typing import Iterable

import pandas as pd

from emailbot.domain_utils import classify_email_domain

__all__ = ["append_foreign_review_sheet"]


def append_foreign_review_sheet(xlsx_path: str, emails: Iterable[str]) -> None:
    """Append or replace the ``Foreign_Review`` sheet in ``xlsx_path``."""

    if not os.path.exists(xlsx_path):
        return
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for email in emails:
        if not email:
            continue
        if email in seen:
            continue
        seen.add(email)
        domain_type = classify_email_domain(email)
        if domain_type in {"global_mail", "foreign_corporate"}:
            rows.append({"email": email, "domain_type": domain_type})
    if not rows:
        return
    df = pd.DataFrame(rows, columns=["email", "domain_type"])
    with pd.ExcelWriter(
        xlsx_path,
        mode="a",
        engine="openpyxl",
        if_sheet_exists="replace",
    ) as writer:
        df.to_excel(writer, sheet_name="Foreign_Review", index=False)
