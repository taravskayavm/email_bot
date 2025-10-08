import re


EMAIL_TOKEN = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"


def normalize_text_for_emails(text: str) -> str:
    """Мягкая нормализация текста перед поиском e-mail адресов.

    Шаги:
    * Снимаем переносы по дефису внутри слов.
    * Превращаем остальные переводы строк в пробелы.
    * Разлепляем токены вида «словоemail@domain» добавлением пробела слева.
    * Схлопываем множественные пробелы.
    Адреса e-mail при этом не модифицируются.
    """

    if not text:
        return text

    # 1) Убрать переносы с дефисом в середине слов.
    text = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", text, flags=re.UNICODE)

    # Остальные переносы превращаем в пробелы.
    text = re.sub(r"\s*\n\s*", " ", text)

    # 2) Вставить пробел перед e-mail, если слева буква/цифра/закрывающий знак.
    # Покрываем латиницу+кириллицу+цифры и частые «закрывающие» (скобки/кавычки/двоеточие/равно):
    #   "Россияnik@site.ru"      → "Россия nik@site.ru"
    #   ")ivanov@site.ru"        → ") ivanov@site.ru"
    #   "E-mail:ivanov@site.ru"  → "E-mail: ivanov@site.ru"
    closing_chars = {")", "]", "»", "”", "'", '"', ":", "="}
    insert_positions = []
    for match in re.finditer(EMAIL_TOKEN, text):
        start = match.start()
        if start == 0:
            continue
        prev_char = text[start - 1]
        if prev_char.isspace():
            continue
        if prev_char.isalpha() or prev_char.isdigit() or prev_char in closing_chars:
            insert_positions.append(start)

    if insert_positions:
        parts = []
        last_idx = 0
        for insert_idx in insert_positions:
            parts.append(text[last_idx:insert_idx])
            parts.append(" ")
            last_idx = insert_idx
        parts.append(text[last_idx:])
        text = "".join(parts)

    # 3) Сжать множественные пробелы.
    text = re.sub(r"[ \t]{2,}", " ", text)

    return text
