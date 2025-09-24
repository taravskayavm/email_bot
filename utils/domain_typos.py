from __future__ import annotations
from __future__ import annotations

import re

_RX = re.compile(r"(?i)\b([a-z0-9._%+-]+)@([a-z0-9.-]+\.[a-z]{2,})\b")

# Conservative mapping of well-known domain typos.
_MAP: dict[str, str] = {
    "gmail.ru": "gmail.com",
    "gnail.com": "gmail.com",
    "gmal.com": "gmail.com",
    "eandex.ru": "yandex.ru",
    "yadnex.ru": "yandex.ru",
}


def autocorrect_domain(addr: str) -> tuple[str, bool, str]:
    """Fix common domain typos without guessing.

    Returns a tuple ``(address, changed?, reason)``. ``reason`` contains a
    ``"old->new"`` string when a replacement occurs.
    """

    s = (addr or "").strip()
    match = _RX.search(s)
    if not match:
        return s, False, ""
    local, domain = match.group(1), match.group(2).lower()
    new_domain = _MAP.get(domain)
    if not new_domain:
        return s, False, ""
    fixed = f"{local}@{new_domain}"
    return fixed, True, f"{domain}->{new_domain}"


__all__ = ["autocorrect_domain"]
