"""Utility helpers for strict e-mail validation and normalization."""

from __future__ import annotations

import re
import unicodedata
from typing import Optional, Tuple

__all__ = [
    "EmailValidationError",
    "clean_and_normalize_email",
]

_ZW_CHARS = (
    "\u200b"  # ZERO WIDTH SPACE
    "\u200c"  # ZERO WIDTH NON-JOINER
    "\u200d"  # ZERO WIDTH JOINER
    "\ufeff"  # ZERO WIDTH NO-BREAK SPACE
)
_TRAIL_PUNCT = ".,;:!?)»”′’…>"
_LEAD_PUNCT = "(«“‘<"

# Локальная часть — строго ASCII, без Unicode. Разрешаем RFC-валидные символы и точки,
# но запрещаем подряд/крайние точки.
LOCAL_ASCII_RE = re.compile(
    r"^[A-Za-z0-9!#$%&'*+/=?^_`{|}~-](?:[A-Za-z0-9!#$%&'*+/=?^_`{|}~.-]{0,62}[A-Za-z0-9!#$%&'*+/=?^_`{|}~-])?$"
)
LOCAL_DOTS_RE = re.compile(r"\.\.")

# Домен — в Unicode, но далее кодируется в IDNA. Валидируем форму до кодирования.

class EmailValidationError(ValueError):
    """Raised when an e-mail fails validation checks."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code

    def __str__(self) -> str:  # pragma: no cover - delegated to base str repr
        return self.code


def _nkfc_trim(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    # убираем нулевой ширины символы и незначащие кавычки/скобки по краям
    s = s.translate({ord(c): None for c in _ZW_CHARS})
    s = s.strip()
    s = s.lstrip(_LEAD_PUNCT).rstrip(_TRAIL_PUNCT)
    return s


def _split_email_parts(addr: str) -> Tuple[str, str]:
    if "@" not in addr:
        raise EmailValidationError("no_at_sign")
    local, domain = addr.rsplit("@", 1)
    if not local or not domain:
        raise EmailValidationError("empty_local_or_domain")
    return local, domain


def _validate_local_ascii(local: str) -> None:
    # локальная часть должна быть ASCII
    try:
        local.encode("ascii")
    except UnicodeEncodeError:
        raise EmailValidationError("local_not_ascii") from None
    if local.startswith(".") or local.endswith("."):
        raise EmailValidationError("local_edge_dot")
    if LOCAL_DOTS_RE.search(local):
        raise EmailValidationError("local_consecutive_dots")
    if not LOCAL_ASCII_RE.match(local):
        raise EmailValidationError("local_bad_chars")


def _validate_unicode_domain_and_to_idna(domain: str) -> str:
    domain = domain.strip()
    if "." not in domain:
        raise EmailValidationError("domain_bad_shape")
    raw_labels = domain.split(".")
    if any(len(label) > 63 for label in raw_labels):
        raise EmailValidationError("domain_label_size")
    # IDNA-кодирование домена с понижением регистра
    try:
        idna = domain.lower().encode("idna").decode("ascii")
    except Exception as exc:  # pragma: no cover - unexpected encoder errors
        raise EmailValidationError("domain_idna_fail") from exc
    if len(idna) > 253:
        raise EmailValidationError("domain_too_long")
    labels = idna.split(".")
    if len(labels) < 2:
        raise EmailValidationError("domain_bad_shape")
    if len(labels[-1]) < 2:
        raise EmailValidationError("domain_bad_shape")
    label_shape = re.compile(r"^[a-z0-9-]+$")
    for label in labels:
        if not label:
            raise EmailValidationError("domain_bad_shape")
        if not 1 <= len(label) <= 63:
            raise EmailValidationError("domain_label_size")
        if label.startswith("-") or label.endswith("-"):
            raise EmailValidationError("domain_label_dash")
        if not label_shape.match(label):
            raise EmailValidationError("domain_bad_shape")
    return idna


def clean_and_normalize_email(raw: str) -> Tuple[Optional[str], Optional[str]]:
    """Return canonical e-mail and optional rejection reason code."""

    s = _nkfc_trim(raw or "")
    # защита от частых ложнопозитивных случаев: «e.g. some.text.» и т.п.
    if s.count("@") != 1:
        return None, EmailValidationError("no_at_sign")
    try:
        local, domain = _split_email_parts(s)
        _validate_local_ascii(local)
        idna_domain = _validate_unicode_domain_and_to_idna(domain)
    except EmailValidationError as exc:
        return None, exc
    # финальный канонический вид: локальная часть как есть (ASCII), домен — IDNA
    return f"{local}@{idna_domain}", None
