import re
import unicodedata
import idna

# Невидимые/служебные символы: ZWSP/ZWNJ/ZWNJ, NBSP, LRM/RLM, WORD JOINER и др.
_ZERO_WIDTH = ''.join(map(chr, [
    0x200B, 0x200C, 0x200D, 0x200E, 0x200F, 0x2060, 0xFEFF
]))
_ZERO_WIDTH_RE = re.compile(f"[{re.escape(_ZERO_WIDTH)}]")

# Внешняя пунктуация, встречающаяся вокруг e-mail при парсинге
_PUNCT_TRIM_RE = re.compile(r'^[\s\(\[\{<«"“”„‚’»>}\]\).,:;]+|[\s\(\[\{<«"“”„‚’»>}\]\).,:;]+$')

# Цифровые сноски (включая надстрочные ¹²³ и пр. circled numbers)
_SUPERSCRIPT_MAP = str.maketrans({
    '¹': '1', '²':'2', '³':'3',
    '⁰':'0','⁴':'4','⁵':'5','⁶':'6','⁷':'7','⁸':'8','⁹':'9',
})
# ①②③…⑳ → 1..20 (нужны хотя бы 1–9)
_CIRCLED_MAP = {chr(cp): str(i) for i, cp in enumerate(range(0x2460, 0x2469), start=1)}

def _normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = s.translate(_SUPERSCRIPT_MAP)
    s = s.translate(str.maketrans(_CIRCLED_MAP))
    s = _ZERO_WIDTH_RE.sub("", s)
    s = s.replace('\xa0', ' ')  # NBSP → space
    return s

_EMAIL_CORE_RE = re.compile(
    r'(?<![\w.+-])'                       # слева не часть слова/email
    r'([A-Za-z0-9._%+\-]+)'               # local
    r'@'
    r'([A-Za-z0-9.-]+\.[A-Za-z]{2,})'     # domain
    r'(?![\w-])'                          # справа не продолжение
)

_ASCII_LOCAL_RE = re.compile(r'^[A-Za-z0-9._%+\-]+$')
_ASCII_DOMAIN_RE = re.compile(r'^[A-Za-z0-9.-]+$')

def extract_emails(text: str) -> list[str]:
    """
    Аккуратный экстрактор: достаёт «чистое ядро» e-mail без внешней пунктуации.
    """
    t = _normalize_text(text)
    out = []
    for m in _EMAIL_CORE_RE.finditer(t):
        local, domain = m.group(1), m.group(2)
        email = f"{local}@{domain}"
        email = _PUNCT_TRIM_RE.sub("", email)
        # частые артефакты после домена (обрывки URL-параметров)
        email = re.sub(r'(\?|\#|/).*$','', email)
        out.append(email.lower())
    return out

_LEADING_FOOTNOTE_RE = re.compile(r'^(?:\d{1,3})+(?=[A-Za-z])')  # 1–3 цифры в начале local-part

def _strip_leading_footnote(local: str) -> str:
    return _LEADING_FOOTNOTE_RE.sub('', local)

def sanitize_email(email: str) -> str:
    """
    Финальная чистка: убираем внешнюю пунктуацию, невидимые символы,
    откусываем ведущие цифры-сноски в local-part, обрезаем крайние -_. от переносов.
    """
    s = _normalize_text(email).lower().strip()
    s = _PUNCT_TRIM_RE.sub("", s)
    s = re.sub(r'(\?|\#|/).*$','', s)

    if '@' not in s:
        return s

    local, domain = s.split('@', 1)
    local = local.replace(',', '.')   # ошибки OCR: запятая вместо точки
    # убираем сноску в начале
    cleaned = _strip_leading_footnote(local)
    # убираем мусорные тире/точки по краям, оставшиеся от переносов
    cleaned = re.sub(r'^[-_.]+|[-_.]+$', '', cleaned)

    # если после удаления цифр осталась валидная локальная часть — используем её
    if cleaned and cleaned != local and _ASCII_LOCAL_RE.match(cleaned):
        local = cleaned

    # Жёстко: local-part строго ASCII, без смешанных скриптов
    if not _ASCII_LOCAL_RE.match(local):
        # отбрасываем — иначе SMTP без SMTPUTF8 не отправит
        return ""

    # Домен: приводим к IDNA (punycode), но запрещаем мусор
    domain = domain.rstrip('.')
    if not _ASCII_DOMAIN_RE.match(domain):
        try:
            domain = idna.encode(domain).decode("ascii")
        except Exception:
            return ""

    return f"{local}@{domain}"

def dedupe_with_variants(emails: list[str]) -> list[str]:
    """
    Дедуплицируем, учитывая пару (сноской)вариант → чистый вариант.
    Если есть и «55alexandr…@» и «alexandr…@», оставляем чистый.
    """
    raw = [sanitize_email(e) for e in emails]
    raw = [e for e in raw if e]  # выбрасываем невалидные
    unique = set(raw)

    # Построим карту «локальная без начальных цифр» → варианты
    bucket = {}
    for e in list(unique):
        if '@' not in e:
            continue
        local, domain = e.split('@', 1)
        key = f"{_strip_leading_footnote(local)}@{domain}"
        bucket.setdefault(key, set()).add(e)

    final = set()
    for key, variants in bucket.items():
        if key in variants:
            # есть чистый — берём только его
            final.add(key)
        else:
            # чистого нет — берём единственный вариант
            # (или самый короткий, если их несколько)
            final.add(sorted(variants, key=len)[0])

    return sorted(final)
