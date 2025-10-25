from __future__ import annotations

from typing import Sequence

from services.ui_state import foreign_allowed_for_batch
from utils.dedupe import unique_keep_order
from utils.geo_domains import is_foreign_email


def pick_for_sending(
    emails: Sequence[str], batch_id: str, chat_id: int | None = None
) -> list[str]:
    """Формирует список к отправке с учётом переключателя 'Иностранные домены'."""

    allow_foreign = foreign_allowed_for_batch(batch_id, chat_id)
    result: list[str] = []
    for email in unique_keep_order(emails):
        if not allow_foreign and is_foreign_email(email):
            continue
        result.append(email)
    return result


__all__ = ["pick_for_sending"]
