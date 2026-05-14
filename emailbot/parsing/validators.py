import os
from functools import lru_cache


@lru_cache(maxsize=4096)
def has_mx(domain: str) -> bool:
    if os.getenv("EMAILBOT_ENABLE_MX_CHECK", "0") != "1":
        return True
    try:
        import dns.resolver  # dnspython
    except Exception:
        return True
    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=3.0)
        return len(answers) > 0
    except Exception:
        return False


def filter_by_mx(emails: set[str]) -> set[str]:
    ok: set[str] = set()
    for e in emails:
        try:
            _, domain = e.rsplit("@", 1)
        except ValueError:
            continue
        if has_mx(domain.lower()):
            ok.add(e)
    return ok
