import os
import re
from typing import Optional

# Флаги из .env
JOIN_EMAIL_BREAKS = os.getenv("JOIN_EMAIL_BREAKS", "1") == "1"
JOIN_HYPHEN_BREAKS = os.getenv("JOIN_HYPHEN_BREAKS", "1") == "1"

# Невидимые/служебные символы
_ZERO_WIDTH = dict.fromkeys(map(ord, "\u200B\u200C\u200D\u2060\uFEFF"), None)  # ZWSP/ZWJ/etc
_SOFT_HYPHEN = "\u00AD"  # мягкий перенос
_NBSP_TO_SPACE = str.maketrans({"\u00A0": " ", "\u202F": " "})

_DASH_CLASS = r"[‐-‒–—―\-]"  # разные «дефисы/тире» + минус

# Универсальные куски e-mail (ASCII; IDNA нормализация — в вашем санитайзере)
_LOCAL_CHARS = r"[A-Za-z0-9._%+\-]"
_DOMAIN_CHARS = r"[A-Za-z0-9.\-]"


def _preclean_obfuscations(s: str) -> str:
    """Снимаем популярные «обфускации» вида name (at) domain [dot] ru, «собака», «точка» и пр."""

    s = re.sub(r"\s*\[?\(?\s*at\s*\)?\]?\s*", "@", s, flags=re.I)
    s = re.sub(r"\s*\[?\(?\s*dog\s*\)?\]?\s*", "@", s, flags=re.I)  # «собака»
    s = re.sub(r"\s*\[?\(?\s*точка\s*\)?\]?\s*", ".", s, flags=re.I)  # «точка»
    s = re.sub(r"\s*\[?\(?\s*dot\s*\)?\]?\s*", ".", s, flags=re.I)
    # убираем пробелы вокруг @ и .
    s = re.sub(rf"({_LOCAL_CHARS})\s*@\s*({_DOMAIN_CHARS})", r"\1@\2", s)
    s = re.sub(rf"({_DOMAIN_CHARS})\s*\.\s*({_DOMAIN_CHARS})", r"\1.\2", s)
    return s


def _strip_invisibles(s: str) -> str:
    if not s:
        return s
    s = s.replace(_SOFT_HYPHEN, "")  # мягкие переносы → убрать
    s = s.translate(_ZERO_WIDTH)  # zero-width символы → убрать
    s = s.translate(_NBSP_TO_SPACE)  # неразрывные пробелы → обычный пробел
    # унифицируем дефисы/тире (если встретятся внутри домена/локальной части)
    s = re.sub(_DASH_CLASS, "-", s)
    # схлопываем множественные пробелы/табуляции
    s = re.sub(r"[ \t]{2,}", " ", s)
    return s


def _join_email_internal_breaks(s: str) -> str:
    """
    Склейка разрывов внутри e-mail:
      ivanov@
        hematology . ru  → ivanov@hematology.ru
      name-
        lastname@...     → namelastname@...
    """

    # 1) переносы строк/лишние пробелы вокруг '@' и '.'
    s = re.sub(rf"({_LOCAL_CHARS})\s*[\r\n]+\s*@", r"\1@", s)  # local\n@ → local@
    s = re.sub(rf"@\s*[\r\n]+\s*({_DOMAIN_CHARS})", r"@\1", s)  # @\n domain → @domain
    s = re.sub(rf"({_DOMAIN_CHARS})\s*[\r\n]+\s*\.?\s*({_DOMAIN_CHARS})", r"\1.\2", s)  # domain\npart → domain.part
    # 2) пробелы внутри «local@domain»
    s = re.sub(rf"({_LOCAL_CHARS})\s+@\s+({_DOMAIN_CHARS})", r"\1@\2", s)
    s = re.sub(rf"({_DOMAIN_CHARS})\s+\.\s+({_DOMAIN_CHARS})", r"\1.\2", s)
    # 3) дефис в конце строки внутри токена (перенос по слогам)
    s = re.sub(rf"({_LOCAL_CHARS})-\s*[\r\n]+\s*({_LOCAL_CHARS})", r"\1\2", s)  # в локальной части
    s = re.sub(rf"({_DOMAIN_CHARS})-\s*[\r\n]+\s*({_DOMAIN_CHARS})", r"\1\2", s)  # в домене
    return s


def _join_soft_hyphen_breaks(s: str) -> str:
    # склейка soft hyphen (если вдруг осталось после _strip_invisibles — дубль-страховка)
    return s.replace(_SOFT_HYPHEN, "")


def fix_email_text(raw: Optional[str]) -> str:
    """
    Полный пайплайн «ремонта» текста перед e-mail regex:
      1) снимаем обфускации,
      2) убираем невидимые символы,
      3) (опц.) склеиваем переносы/дефисы внутри адресов.
    Возвращает обычную текстовую строку, пригодную для стандартного поиска e-mail.
    """

    if not raw:
        return ""
    s = str(raw)
    s = _preclean_obfuscations(s)
    s = _strip_invisibles(s)
    if JOIN_EMAIL_BREAKS:
        s = _join_email_internal_breaks(s)
    if JOIN_HYPHEN_BREAKS:
        s = _join_soft_hyphen_breaks(s)
    return s
