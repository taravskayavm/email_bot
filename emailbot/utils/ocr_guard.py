"""Utility helpers for lightweight OCR capability checks."""

from __future__ import annotations

import os
import shutil
from typing import Any

from emailbot.config import (  # Импортируем настройки по умолчанию для OCR
    PDF_OCR_MIN_CHARS,  # Подтягиваем лимит минимального числа символов
    PDF_OCR_MIN_TEXT_RATIO,  # Забираем долю текстовых страниц для эвристики
    PDF_OCR_PROBE_PAGES,  # Берём количество страниц для проверки
    TESSERACT_CMD,  # Загружаем путь к бинарю tesseract из конфига
)


def ocr_available(tesseract_cmd: str | None = None) -> bool:  # Определяем проверку доступности OCR
    """Check whether pytesseract and the ``tesseract`` binary are ready for use."""  # Объясняем поведение функции

    try:  # Пытаемся импортировать pytesseract как обязательную зависимость
        import pytesseract  # type: ignore  # noqa: F401  # Подтверждаем наличие модуля без использования
    except Exception:  # Обрабатываем отсутствие pytesseract в окружении
        return False  # Возвращаем False, если библиотека не найдена

    override_path = (tesseract_cmd or TESSERACT_CMD or "").strip()  # Собираем путь к бинарю из приоритетных источников
    if override_path:  # Проверяем, указан ли явный путь
        return os.path.exists(override_path)  # Подтверждаем, что бинарь доступен по указанному пути
    binary_path = shutil.which("tesseract")  # Ищем tesseract в PATH, если явного пути нет
    return binary_path is not None  # Возвращаем True, когда бинарь найден в PATH


def needs_ocr(  # Объявляем эвристику определения необходимости OCR
    doc: Any,  # Принимаем открытый документ PyMuPDF или аналогичный объект
    *,
    probe_pages: int | None = None,  # Позволяем переопределить число страниц для проверки
    min_chars: int | None = None,  # Даём возможность задать порог символов
    min_ratio: float | None = None,  # Разрешаем переопределить долю текстовых страниц
) -> bool:  # Возвращаем логический ответ, требуется ли OCR
    """Estimate if OCR is required by sampling several pages from ``doc``."""  # Кратко описываем алгоритм

    try:  # Пробуем определить количество страниц в документе
        total_pages = len(doc)  # Вычисляем длину объекта документа
    except Exception:  # Если документ не поддерживает len()
        return True  # Считаем, что OCR нужен, так как нет уверенности в содержимом

    if total_pages <= 0:  # Проверяем, что документ не пустой
        return True  # Пустой документ трактуем как кандидат на OCR

    resolved_probe = max(  # Вычисляем число проверяемых страниц с ограничениями
        1,  # Минимум одна страница для анализа
        min(  # Ограничиваем верхний предел
            (probe_pages or PDF_OCR_PROBE_PAGES),  # Берём пользовательский или дефолтный размер пробы
            total_pages,  # Не превышаем фактическое количество страниц
        ),
    )
    resolved_min_chars = int(  # Определяем порог символов для текстовой страницы
        min_chars or PDF_OCR_MIN_CHARS  # Используем пользовательское или дефолтное значение
    )
    resolved_min_ratio = float(  # Устанавливаем требуемую долю текстовых страниц
        min_ratio or PDF_OCR_MIN_TEXT_RATIO  # Предпочитаем явный параметр или значение из конфига
    )
    text_pages = 0  # Счётчик страниц, которые содержат достаточно текста

    for index in range(resolved_probe):  # Перебираем выбранные страницы
        try:  # Пытаемся получить текст текущей страницы
            page_text = doc[index].get_text("text") or ""  # Извлекаем текст, заменяя None на пустую строку
        except Exception:  # Ловим возможные ошибки получения текста
            page_text = ""  # В случае ошибки считаем страницу пустой
        if len(page_text) >= resolved_min_chars:  # Проверяем, удовлетворяет ли страница порогу символов
            text_pages += 1  # Увеличиваем счётчик текстовых страниц

    ratio = text_pages / float(resolved_probe)  # Рассчитываем долю текстовых страниц
    return ratio < resolved_min_ratio  # Возвращаем True, если доля ниже порога и OCR потребуется


__all__ = ["ocr_available", "needs_ocr"]  # Экспортируем доступные функции модуля
