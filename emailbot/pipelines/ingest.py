"""Ingest helpers for bulk address validation."""

from __future__ import annotations

from collections import Counter
from typing import Iterable, List, Tuple

from emailbot.utils.email_clean import clean_and_normalize_email

__all__ = ["ingest_emails"]


def ingest_emails(lines: Iterable[str]) -> Tuple[List[str], List[str], dict[str, object]]:
    """Validate ``lines`` with strict checks and return (ok, bad, stats)."""

    source = list(lines)
    seen: set[str] = set()
    ok: List[str] = []
    bad: List[str] = []
    rejects: Counter[str] = Counter()

    for raw in source:
        email, reason = clean_and_normalize_email(raw)
        if email is None:
            bad.append(raw)
            if reason:
                rejects[str(reason)] += 1
            else:  # pragma: no cover - defensive, all paths provide a reason
                rejects["unknown"] += 1
            continue
        if email in seen:
            continue
        seen.add(email)
        ok.append(email)

    stats = {
        "total_in": len(source),
        "ok": len(ok),
        "bad": len(bad),
        "rejects": dict(rejects),
    }
    return ok, bad, stats
