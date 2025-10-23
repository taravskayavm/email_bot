import os
import re
from email.utils import parseaddr
from unicodedata import normalize as uni_norm

import idna

STRICT_DOMAIN_VALIDATE = os.getenv("STRICT_DOMAIN_VALIDATE", "1") == "1"
CONFUSABLES_NORMALIZE = os.getenv("CONFUSABLES_NORMALIZE", "1") == "1"
IDNA_DOMAIN_NORMALIZE = os.getenv("IDNA_DOMAIN_NORMALIZE", "1") == "1"

LOCAL_CONFUSABLES = str.maketrans(
    {
        "е": "e",
        "о": "o",
        "с": "c",
        "р": "p",
        "а": "a",
        "х": "x",
        "у": "y",
        "к": "k",
        "Е": "E",
        "О": "O",
        "С": "C",
        "Р": "P",
        "А": "A",
        "Х": "X",
        "У": "Y",
        "К": "K",
    }
)

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _idna(domain: str) -> str:
    try:
        return idna.encode(domain, uts46=True).decode("ascii")
    except Exception:
        return domain


def normalize_email(raw: str) -> str | None:
    name, addr = parseaddr(raw or "")
    del name
    addr = addr.strip().lower()
    if not addr:
        return None
    if CONFUSABLES_NORMALIZE:
        local, sep, domain = addr.partition("@")
        local = uni_norm("NFKC", local).translate(LOCAL_CONFUSABLES)
        domain = uni_norm("NFKC", domain)
        if IDNA_DOMAIN_NORMALIZE and domain:
            domain = _idna(domain)
        addr = f"{local}{sep}{domain}" if domain else addr
    if STRICT_DOMAIN_VALIDATE and not EMAIL_RE.match(addr):
        return None
    return addr
