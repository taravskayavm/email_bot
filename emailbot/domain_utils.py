from __future__ import annotations
import re
from functools import lru_cache

# Домены массовых почтовых сервисов, которые не считаем «иностранными корпоративными»
GLOBAL_MAIL_PROVIDERS = {
    # глобальные
    "gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com",
    "yahoo.com", "icloud.com", "proton.me", "protonmail.com", "mail.com",
    "gmx.com", "aol.com", "msn.com", "pm.me",
    # привычные для РФ
    "yandex.com", "yandex.ru", "ya.ru", "mail.ru", "bk.ru", "list.ru",
    "inbox.ru", "rambler.ru", "lenta.ru",
}

# TLD, которые считаем «RU-подобными»
RU_LIKE_TLDS = {"ru", "рф", "su"}

_re_email = re.compile(r"^[^@\s]+@([A-Za-z0-9.-]+\.[A-Za-z0-9-]+)$")

@lru_cache(maxsize=8192)
def classify_email_domain(email: str) -> str:
    """
    Возвращает одну из меток:
      - 'ru_like'            — домен в RU/РФ/SU зонах
      - 'global_mail'        — массовый почтовик (gmail/outlook/.../mail.ru и т.д.)
      - 'foreign_corporate'  — остальные домены вне RU-подобных (то, что действительно считаем «иностранными доменами»)
      - 'unknown'            — не удалось распарсить домен
    """
    if not email or "@" not in email:
        return "unknown"
    m = _re_email.match(email.strip().lower())
    if not m:
        return "unknown"
    domain = m.group(1)
    if domain in GLOBAL_MAIL_PROVIDERS:
        return "global_mail"
    tld = domain.rsplit(".", 1)[-1]
    if tld in RU_LIKE_TLDS:
        return "ru_like"
    return "foreign_corporate"


def count_domains(emails: list[str]) -> dict[str, int]:
    res = {"ru_like": 0, "global_mail": 0, "foreign_corporate": 0, "unknown": 0}
    for e in emails:
        res[classify_email_domain(e)] += 1
    return res
