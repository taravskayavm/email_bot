from __future__ import annotations

from typing import Iterable

from config import LOCAL_TLDS, LOCAL_DOMAINS_EXTRA


def email_domain(email: str) -> str:
    try:
        at = email.rindex("@")
    except ValueError:
        return ""
    return email[at + 1 :].strip().lower()


def is_foreign_email(email: str) -> bool:
    d = email_domain(email)
    if not d:
        return False
    # allow-list доменов — считаем локальными
    if d in LOCAL_DOMAINS_EXTRA:
        return False
    # локальные TLD — считаем локальными
    for suf in LOCAL_TLDS:
        suf = suf.strip().lower()
        if suf and d.endswith(suf):
            return False
    return True


def split_foreign(emails: Iterable[str]) -> tuple[list[str], list[str]]:
    local, foreign = [], []
    for e in emails:
        (foreign if is_foreign_email(e) else local).append(e)
    return local, foreign
