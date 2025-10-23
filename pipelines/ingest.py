from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, TypeVar

from utils.email_clean import sanitize_email, dedupe_with_variants, _strip_leading_footnote

logger = logging.getLogger(__name__)

T = TypeVar("T")


def ingest(all_extracted_emails: list[str]) -> tuple[list[str], str]:
    cleaned = [sanitize_email(e)[0] for e in all_extracted_emails]
    rejected_non_ascii = sum(1 for e in cleaned if not e)
    cleaned = [e for e in cleaned if e]  # убираем невалидные
    emails = dedupe_with_variants(cleaned)

    before = set(all_extracted_emails)              # до sanitize+dedupe
    # приблизительная оценка «сносочных» — сколько адресов пропали лишь из-за варианта с ведущими цифрами
    def _key(e: str) -> str:
        local, domain = e.split('@', 1)
        return f"{_strip_leading_footnote(local)}@{domain}"

    before_keys = {_key(e) for e in before}
    lost_as_variants = len(before) - len(before_keys)
    footnote_removed = max(0, lost_as_variants)

    found = len(before)
    stats = (
        f"✅ Анализ завершён.\n"
        f"Найдено адресов: {found}\n"
        f"Уникальных (после очистки): {len(emails)}\n"
        f"Отклонены (не-ASCII локальная часть): {rejected_non_ascii}\n"
        f"Возможные сносочные дубликаты удалены: {footnote_removed}"
    )

    return emails, stats


def process_files(paths: list[Path], process_one: Callable[[Path], T]) -> list[T]:
    """Process ``paths`` sequentially while logging successes and failures."""

    results: list[T] = []
    for path in paths:
        try:
            results.append(process_one(path))
        except Exception as exc:  # pragma: no cover - diagnostic logging only
            logger.exception("Skip %s: %s", path, exc)
        else:
            try:
                size = path.stat().st_size
            except OSError:
                size = -1
            logger.info("Parsed: %s (ext=%s, size=%d)", path.name, path.suffix, size)
    return results
