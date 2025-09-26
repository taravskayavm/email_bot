from __future__ import annotations

import socket
from functools import lru_cache

try:  # pragma: no cover - optional dependency branch
    import dns.resolver  # type: ignore

    _HAS_DNS = True
    _DNS_NEGATIVE_EXC = tuple(
        exc
        for exc in (
            getattr(dns.resolver, "NXDOMAIN", None),
            getattr(dns.resolver, "NoAnswer", None),
        )
        if isinstance(exc, type)
    )
except Exception:  # pragma: no cover - fallback path
    _HAS_DNS = False
    _DNS_NEGATIVE_EXC: tuple[type[Exception], ...] = ()

_GAI_NEGATIVE_ERRNOS = {
    getattr(socket, "EAI_NONAME", None),
    getattr(socket, "EAI_NODATA", None),
}
_GAI_NEGATIVE_ERRNOS.discard(None)


@lru_cache(maxsize=2048)
def domain_has_mx(domain: str, timeout: float = 2.0) -> bool:
    """Return ``True`` if ``domain`` appears to accept mail."""

    d = (domain or "").strip().lower()
    if not d:
        return False
    if _HAS_DNS:
        try:
            dns.resolver.resolve(d, "MX", lifetime=timeout)
            return True
        except Exception as exc:
            if _DNS_NEGATIVE_EXC and isinstance(exc, _DNS_NEGATIVE_EXC):
                return False
            # fall back to basic socket check below
    try:
        infos = socket.getaddrinfo(d, 25, proto=socket.IPPROTO_TCP)
        return bool(infos)
    except socket.gaierror as exc:  # pragma: no cover - depends on system resolver
        if exc.errno in _GAI_NEGATIVE_ERRNOS:
            return False
        return True
    except Exception:
        return True


__all__ = ["domain_has_mx"]
