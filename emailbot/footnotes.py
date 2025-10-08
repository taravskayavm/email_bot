from __future__ import annotations

import re

# Буквенные сноски, часто встречаются как надстрочные: aᵃ, bᵇ, cᶜ или [a], (a)
SUPERSCRIPTS = "\u00B9\u00B2\u00B3\u2070-\u209F"  # диапазоны надстрочных/подстрочных
EMAIL_TOKEN = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"
EMAIL_PLACEHOLDER = r"\x00E\d+\x00"

_re_email = re.compile(EMAIL_TOKEN)


def _mask_emails(text: str) -> tuple[str, list[str]]:
    """
    Заменяем e-mail на плейсхолдеры, возвращаем (новый_текст, список_адресов).
    Это защищает адреса от любых чисток сносок.
    """
    emails: list[str] = []

    def _sub(m):
        idx = len(emails)
        emails.append(m.group(0))
        return f"\x00E{idx}\x00"

    return _re_email.sub(_sub, text), emails


def _unmask_emails(text: str, emails: list[str]) -> str:
    for i, addr in enumerate(emails):
        text = text.replace(f"\x00E{i}\x00", addr)
    return text


def remove_footnotes_safe(text: str) -> str:
    """
    Безопасное удаление сносок:
    - Маскируем e-mail перед обработкой (чтобы не повредить адреса, включая начинающиеся с цифр).
    - Удаляем надстрочные/подстрочные символы после слова.
    - Удаляем скобочные сноски (a), [b], {c}.
    - Удаляем «впритык»-сноски без скобок после слова (a|b|c|1|2|3 перед пробелом/знаком препинания).
    - Удаляем «впритык»-сноску, если она сразу перед e-mail.
    - Размаскируем e-mail.
    """
    if not text:
        return text

    # 0) Маскируем e-mail
    text, masked = _mask_emails(text)

    # 1) Удаляем надстрочные/подстрочные индексы после слова
    text = re.sub(rf"(\w)[{SUPERSCRIPTS}]+", r"\1", text)

    # 2) Удаляем (a)/(b)/(c) и [a]/[b]/[c] как сноски (не трогаем e-mail)
    text = re.sub(r"(?<=\w)\s*[\(\[\{]\s*[a-cA-C]\s*[\)\]\}](?!\s*@)", "", text)

    # 2.1) Удалить «впритык»-сноску, если она сразу перед e-mail (замаскированным или «живым»)
    text = re.sub(
        rf"(?<=\w)([a-cA-C1-3])(?={EMAIL_PLACEHOLDER})", "", text
    )
    text = re.sub(
        rf"(?<=\w)([a-cA-C1-3])(?={EMAIL_TOKEN})", "", text
    )

    # 3) «Впритык»-сноски без скобок после слова:
    #    Пример: 'Россияa', 'Иванов2' → удалить 'a'/'2', если за ними пробел/пунктуация/конец.
    #    При этом не трогаем ситуации, когда после пробела/переноса сразу начинается e-mail —
    #    это не сноска, а «прилипший» local-part, который затем будет разлеплен.
    email_follow = rf"(?!\s*(?:-?\s*)?(?:{EMAIL_PLACEHOLDER}|{EMAIL_TOKEN}))"
    tight_patterns = [
        (
            r"(?<=[А-Яа-яЁё])(?P<foot>[a-cA-C])(?=(?:\s|[.,;:!?)\]\}»]|$))"
            + email_follow,
            "",
        ),
        (
            r"(?<=[А-Яа-яЁё])(?P<foot>[123])(?=(?:\s|[.,;:!?)\]\}»]|$))" + email_follow,
            "",
        ),
        (
            r"(?<=\w)(?P<foot>[a-cA-C])(?=(?:\s|[.,;:!?)\]\}»]|$))" + email_follow,
            "",
        ),
        (
            r"(?<=\w)(?P<foot>[123])(?=(?:\s|[.,;:!?)\]\}»]|$))" + email_follow,
            "",
        ),
    ]
    for pat, repl in tight_patterns:
        text = re.sub(pat, repl, text)

    # 4) Размаскируем e-mail обратно
    return _unmask_emails(text, masked)
