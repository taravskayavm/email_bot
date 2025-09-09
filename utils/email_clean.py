import re
import unicodedata
import idna
from functools import lru_cache

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

_HOMO_LATIN = str.maketrans({
    # кириллические «похожие» → латиница (только для детекции!)
    "а": "a", "е": "e", "о": "o", "р": "p", "с": "c", "х": "x",
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H",
    "О": "O", "Р": "P", "С": "C", "Т": "T", "Х": "X",
})

def _latinize_for_detection(s: str) -> str:
    """Только для поиска границ: переводим гомоглифы в латиницу."""
    try:
        return s.translate(_HOMO_LATIN)
    except Exception:
        return s

_OBF_AT = [
    r"\[at\]", r"\(at\)", r"\{at\}", r"\sat\s", r"\s@\s", r"\sat\s",
    r"\[собака\]", r"\(собака\)", r"\{собака\}", r"\sсобака\s",
]
_OBF_DOT = [
    r"\[dot\]", r"\(dot\)", r"\{dot\}", r"\sdot\s",
    r"\[точка\]", r"\(точка\)", r"\{точка\}", r"\sточка\s",
]

@lru_cache(maxsize=256)
def _deobfuscate(text: str) -> str:
    """
    Простейшая размаскировка: user [at] site [dot] ru → user@site.ru
    Поддерживает англ./рус. маркеры и произвольные пробелы/скобки.
    """
    t = text
    # унификация пробелов
    t = re.sub(r"\s+", " ", t)
    # замены at
    for pat in _OBF_AT:
        t = re.sub(pat, " @ ", t, flags=re.IGNORECASE)
    # замены dot
    for pat in _OBF_DOT:
        t = re.sub(pat, " . ", t, flags=re.IGNORECASE)
    # сжать пробелы и убрать их вокруг разделителей
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s*@\s*", "@", t)
    t = re.sub(r"\s*\.\s*", ".", t)
    # частые OCR-ошибки: запятая перед TLD
    t = re.sub(r"@([^,\s]+),([A-Za-z]{2,})\b", r"@\1.\2", t)
    return t

def _normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = s.translate(_SUPERSCRIPT_MAP)
    s = s.translate(str.maketrans(_CIRCLED_MAP))
    # 1) удаляем невидимые символы (ZWSP, LRM и т.п.)
    s = _ZERO_WIDTH_RE.sub("", s)
    # 2) заменяем переносы строк, табы и NBSP на пробел
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\n", " ").replace("\t", " ")
    s = s.replace("\xa0", " ")
    # 2.0) размаскировка "at/dot/собака/точка" перед границами
    s = _deobfuscate(s)
    # 2.1) если e-mail прилип к предыдущему слову (в т.ч. кириллица-гомоглифы) — вставим пробел
    s_det = _latinize_for_detection(s)
    s_det = re.sub(
        r"([^\s<>\(\)\[\]\{\}])([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})",
        r"\1 \2",
        s_det,
    )
    # переносим "вставленные" пробелы обратно по длине
    if len(s_det) == len(s):
        s = s_det
    # 2.2) и если e-mail слит со следующим словом (редко встречается)
    s_det = _latinize_for_detection(s)
    s_det = re.sub(
        r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})([^\s<>\(\)\[\]\{\}])",
        r"\1 \2",
        s_det,
    )
    if len(s_det) == len(s):
        s = s_det
    # 3) сжимаем повторяющиеся пробелы
    s = re.sub(r" {2,}", " ", s)
    return s

_EMAIL_CORE_RE = re.compile(
    r'(?<![A-Za-z0-9._%+-])'          # слева не часть слова/email
    r'([A-Za-z0-9._%+-]+)'
    r'@'
    r'([A-Za-z0-9.-]+\.[A-Za-z]{2,})' # домен
    r'(?![A-Za-z0-9.-])',             # справа не продолжение
    re.IGNORECASE
)

_ASCII_LOCAL_RE = re.compile(r'^[A-Za-z0-9._%+\-]+$')
_ASCII_DOMAIN_RE = re.compile(r'^[A-Za-z0-9.-]+$')

_TLD_PREFIXES = (
    "ru", "com", "net", "org", "gov", "edu", "info", "biz", "su", "ua", "рф",
)

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

def sanitize_email(email: str, strip_footnote: bool = True) -> str:
    """
    Финальная чистка: убираем внешнюю пунктуацию, невидимые символы,
    откусываем ведущие цифры-сноски в local-part, обрезаем крайние -_. от переносов.
    """
    s = _normalize_text(email).lower().strip()
    s = _PUNCT_TRIM_RE.sub("", s)
    s = re.sub(r"(\?|\#|/).*$", "", s)

    if "@" not in s:
        return ""

    local, domain = s.split("@", 1)
    local = local.replace(",", ".")  # ошибки OCR: запятая вместо точки
    # убираем ведущие цифры-сноски, если требуется
    if strip_footnote:
        local = _strip_leading_footnote(local)
    # чистим края от .-_ оставшихся от переносов
    local = re.sub(r"^[-_.]+|[-_.]+$", "", local)

    # жёстко: local-part строго ASCII
    if not _ASCII_LOCAL_RE.match(local):
        return ""

    # If local part accidentally contains something that looks like a
    # domain with a known top-level domain followed by additional
    # characters (e.g. ``mail.ruovalov``), it is likely the result of two
    # concatenated addresses and should be rejected.
    for tld in _TLD_PREFIXES:
        # detect e.g. ``mail.ruovalov`` where ``.ru`` is followed by more letters
        if re.search(rf"\.{tld}[A-Za-z]", local):
            return ""

    # домен: приводим к IDNA (punycode), но запрещаем мусор
    domain = domain.rstrip(".")
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
    clean = [sanitize_email(e) for e in emails]
    variants = [sanitize_email(e, strip_footnote=False) for e in emails]

    pairs = [(c, v) for c, v in zip(clean, variants) if v]

    bucket: dict[str, set[str]] = {}
    for c, v in pairs:
        bucket.setdefault(c, set()).add(v)

    final = set()
    for key, vars_set in bucket.items():
        if key in vars_set:
            final.add(key)
        elif len(vars_set) == 1:
            # only one variant – assume digits were footnotes and strip them
            final.add(key)
        else:
            # multiple variants without a clean version: keep the shortest variant
            final.add(sorted(vars_set, key=len)[0])

    return sorted(final)


def parse_manual_input(text: str) -> list[str]:
    """
    Унифицированный парсер ручного ввода.
    Использует тот же пайплайн, что и для файлов/сайтов:
      extract_emails → sanitize_email → dedupe_with_variants
    """
    raw = extract_emails(text)
    cleaned = [e for e in (sanitize_email(x) for x in raw) if e]
    return dedupe_with_variants(cleaned)
