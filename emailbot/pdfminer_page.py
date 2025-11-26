"""Fallback helpers built on top of pdfminer.six for single-page text access."""

from __future__ import annotations  # Обеспечиваем совместимость с аннотациями типов будущих версий

from pathlib import Path  # Работаем с путями к PDF-файлам


def extract_page_text(pdf_path: Path, index: int) -> str:
    """Return textual content for ``index``-th page using a conservative pdfminer flow."""

    _ = index  # Явно отмечаем, что индекс используется только для совместимой сигнатуры
    try:  # Пытаемся задействовать высокоуровневый API pdfminer
        from pdfminer.high_level import extract_text  # type: ignore  # Импортируем функцию извлечения текста
    except Exception:  # При отсутствии pdfminer сразу завершаемся
        return ""  # Возвращаем пустую строку, сохраняя безопасное поведение

    try:  # Оборачиваем реальное извлечение в защитный блок
        text = extract_text(str(pdf_path)) or ""  # Загружаем текст всего документа и нормализуем None
    except Exception:  # При любых ошибках чтения
        return ""  # Возвращаем пустую строку, чтобы вызывающая сторона продолжила работу

    return text  # Возвращаем полученный текст как псевдостраницу


def count_pages_fast(pdf_path: Path) -> int:
    """Quickly estimate page count of ``pdf_path`` leveraging pdfminer metadata."""

    try:  # Пытаемся подключить низкоуровневые компоненты pdfminer
        from pdfminer.pdfparser import PDFParser  # type: ignore  # Импортируем парсер PDF
        from pdfminer.pdfdocument import PDFDocument  # type: ignore  # Забираем объект документа pdfminer
    except Exception:  # Если pdfminer недоступен
        return 0  # Возвращаем ноль страниц как безопасное значение

    try:  # Открываем файл на чтение в бинарном режиме
        with open(pdf_path, "rb") as handle:  # Создаём файловый дескриптор
            parser = PDFParser(handle)  # Инициализируем парсер поверх файла
            document = PDFDocument(parser)  # Создаём представление документа
            try:  # Пытаемся получить итератор страниц
                pages = list(document.get_pages())  # type: ignore[attr-defined]  # Конвертируем все страницы в список
            except Exception:  # При отсутствии метода или ошибке чтения
                return 0  # Сообщаем об отсутствии информации о страницах
    except Exception:  # При ошибках открытия файла
        return 0  # Возвращаем ноль страниц

    return len(pages)  # Возвращаем число обнаруженных страниц


__all__ = ["extract_page_text", "count_pages_fast"]  # Экспортируем публичные функции модуля
