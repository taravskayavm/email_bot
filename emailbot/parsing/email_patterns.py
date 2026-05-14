import re
import idna

_AT_VARIANTS = [
    r"\s*\[\s*at\s*\]\s*", r"\s*\(\s*at\s*\)\s*", r"\s*\{\s*at\s*\}\s*",
    r"\s+at\s+", r"\s+собака\s+", r"\s*@\s*"
]
_DOT_VARIANTS = [
    r"\s*\[\s*dot\s*\]\s*", r"\s*\(\s*dot\s*\)\s*", r"\s*\{\s*dot\s*\}\s*",
    r"\s+dot\s+", r"\s+точка\s+", r"\s*\.\s*"
]

# Локальная часть: буквы/цифры/._%+- + Юникод; домен: label(Юникод) + точки; TLD ≥2
EMAIL_CORE = r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}"
EMAIL_CORE_UNI = r"[^\s@]+@[^\s@]+\.[^\s@]{2,}"
EMAIL_RE = re.compile(EMAIL_CORE, re.IGNORECASE)
EMAIL_RE_UNI = re.compile(EMAIL_CORE_UNI, re.IGNORECASE)

def _collapse_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def deobfuscate(text: str) -> str:
    s = text
    for pat in _AT_VARIANTS:
        s = re.sub(pat, "@", s, flags=re.IGNORECASE)
    for pat in _DOT_VARIANTS:
        s = re.sub(pat, ".", s, flags=re.IGNORECASE)
    # убрать пробелы вокруг специальных символов
    s = re.sub(r"\s*@\s*", "@", s)
    s = re.sub(r"\s*\.\s*", ".", s)
    return _collapse_ws(s)

def _idna_normalize(email: str) -> str:
    try:
        local, domain = email.split("@", 1)
        # idna алфавит только для домена
        domain_ascii = idna.encode(domain.strip()).decode("ascii")
        return f"{local.strip()}@{domain_ascii}"
    except Exception:
        return email.strip()

def extract_emails(text: str) -> set[str]:
    if not text:
        return set()
    raw = deobfuscate(text)
    found = set()
    # Сначала строгий ASCII-шаблон, затем — более мягкий Unicode
    for m in EMAIL_RE.findall(raw):
        found.add(m.strip())
    for m in EMAIL_RE_UNI.findall(raw):
        found.add(m.strip())
    # Нормализуем домены в Punycode и фильтруем очевидный мусор
    clean = set()
    for e in found:
        e2 = _idna_normalize(e)
        if ".." in e2 or e2.count("@") != 1:
            continue
        if e2.lower().startswith(("mailto:", "at:", "e-mail:", "email:")):
            e2 = re.sub(r"^(mailto:|e-?mail:|at:)\s*", "", e2, flags=re.I)
        clean.add(e2.lower())
    return clean
