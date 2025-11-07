"""Lightweight helpers for quick PDF scans using PyMuPDF."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Set

from emailbot.config import PDF_MAX_PAGES, PARSE_COLLECT_ALL  # Импортируем лимиты для быстрой обработки
from emailbot.cancel_token import is_cancelled
from emailbot.utils.text_preprocess import normalize_for_email  # Подключаем нормализацию текста под e-mail


def extract_emails_from_pdf_fast_core(
    doc,  # Принимаем открытый PDF-документ из PyMuPDF или аналогичного API
    *,
    limit_pages: Optional[int] = None,  # Позволяем ограничить число страниц для быстрого прохода
    target: Optional[int] = None,  # Даём возможность выйти при достижении количества адресов
    progress: Optional[object] = None,  # Поддерживаем обновление прогресса внешнему наблюдателю
) -> Set[str]:  # Возвращаем множество найденных адресов без дубликатов
    """Iterate over ``doc`` pages, respecting limits, and collect e-mails quickly."""  # Объясняем алгоритм функции

    from emailbot.parsing.extract_from_text import emails_from_text  # Импортируем ленивый парсер текста

    found: Set[str] = set()  # Подготавливаем контейнер для уникальных адресов
    try:  # Пытаемся получить число страниц напрямую
        total_pages = getattr(doc, "page_count", len(doc))  # Предпочитаем свойство page_count, затем len()
    except Exception:  # В случае отсутствия метаданных
        total_pages = 0  # Считаем количество страниц неизвестным
    page_limit = total_pages if total_pages else 0  # Запоминаем базовый предел обхода
    if limit_pages is not None:  # Проверяем, задан ли явный лимит страниц
        try:  # Стараемся безопасно привести лимит к int
            requested_limit = max(0, int(limit_pages))  # Отбрасываем отрицательные значения
        except Exception:  # Если привести не получилось
            requested_limit = 0  # Сбрасываемся на ноль как безопасное значение
        if requested_limit:  # Если лимит корректен и положителен
            if page_limit == 0:  # Проверяем, известен ли базовый предел
                page_limit = requested_limit  # Используем явный лимит, если общего значения не было
            else:  # Когда базовый предел присутствует
                page_limit = min(page_limit, requested_limit)  # Уточняем фактический предел обхода
    if PDF_MAX_PAGES and page_limit:  # Учитываем глобальный предел из конфигурации
        page_limit = min(page_limit, PDF_MAX_PAGES)  # Не превышаем общий лимит страниц
    elif PDF_MAX_PAGES and not page_limit:  # Если локального лимита нет, но глобальный присутствует
        page_limit = PDF_MAX_PAGES  # Используем глобальный предел как основной
    if progress:  # При наличии трекера прогресса
        try:  # Оборачиваем вызовы в защиту от ошибок
            if page_limit:  # Если известно число страниц к обработке
                progress.set_total(page_limit)  # Передаём ожидаемое количество страниц
        except Exception:  # Игнорируем любые ошибки обновления прогресса
            pass
        try:
            progress.set_phase("PDF")  # Сообщаем фазу обработки
        except Exception:
            pass
        try:
            progress.set_found(len(found))  # Инициализируем счётчик найденных адресов
        except Exception:
            pass
    max_index = page_limit if page_limit else total_pages  # Определяем фактическое количество итераций
    max_index = max_index if max_index else 0  # Нормализуем None в ноль
    for index in range(max_index):  # Перебираем страницы по индексам
        if is_cancelled():  # Прерываемся, если глобальный токен отмены активирован
            break  # Останавливаем обработку по сигналу отмены
        if progress:  # При наличии трекера прогресса
            try:
                if getattr(progress, "is_cancelled", lambda: False)():  # Проверяем внешний сигнал отмены
                    break  # Выходим из цикла при явной отмене от наблюдателя
            except Exception:
                pass
        try:  # Пробуем получить страницу по индексу
            page = doc[index]  # Извлекаем страницу из документа
        except Exception:  # Если страница не читается
            continue  # Пропускаем проблемную страницу
        try:  # Пытаемся вычитать текст в виде обычного текста
            raw_text = page.get_text("text") or ""  # Получаем текст, заменяя None на пустую строку
        except Exception:  # В случае ошибок рендеринга текста
            raw_text = ""  # Считаем страницу пустой
        text = normalize_for_email(raw_text)  # Подготавливаем текст к выделению адресов
        if text:  # Проверяем, есть ли материал для анализа
            found |= emails_from_text(text)  # Добавляем найденные адреса в множество
        if progress:  # Обновляем прогресс после обработки страницы
            try:
                progress.inc_pages(1)  # Увеличиваем счётчик обработанных страниц
                progress.set_found(len(found))  # Сообщаем текущее количество найденных адресов
            except Exception:
                pass
        if target is not None and len(found) >= target:  # Прекращаем раннюю обработку при достижении цели
            break  # Досрочно завершаем цикл при выполнении цели по количеству адресов
        if not PARSE_COLLECT_ALL and len(found) >= 10:  # Сохраняем прежнее поведение раннего выхода
            break  # Прерываемся после сбора достаточного количества адресов
    return found  # Возвращаем множество уникальных e-mail адресов


def extract_emails_fitz(
    pdf_path: Path,
    progress: Optional[object] = None,
) -> Set[str]:
    """Extract a handful of e-mails using PyMuPDF if available."""

    try:
        import fitz  # type: ignore
        from emailbot.parsing.extract_from_text import emails_from_text
    except Exception:
        return set()

    try:
        doc = fitz.open(str(pdf_path))
    except Exception:
        return set()

    try:
        if progress:
            try:
                total = getattr(doc, "page_count", len(doc))
            except Exception:
                total = 0
            if total:
                try:
                    progress.set_total(total)
                except Exception:
                    pass
        found = extract_emails_from_pdf_fast_core(doc, progress=progress)
    finally:
        try:
            doc.close()
        except Exception:
            pass
    return found
