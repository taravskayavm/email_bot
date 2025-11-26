"""Helpers for speedy PDF scans with careful fallbacks and progress updates."""

from __future__ import annotations  # Подключаем будущее поведение аннотаций

import time  # Работаем со временем для обновления прогресса
from pathlib import Path  # Оперируем путями к файлам PDF
from typing import Optional, Set  # Используем необязательные аргументы и множества

from emailbot.config import (  # Импортируем конфигурацию для управления поведением парсера
    PDF_FAST_LIMIT_PAGES,  # Получаем лимит страниц для быстрого профиля
    PDF_FOUND_TARGET,  # Забираем целевой порог количества адресов
    PDF_MAX_PAGES,  # Сохраняем глобальный предел страниц
    PDF_PAGE_TIMEOUT_SEC,  # Ограничиваем время извлечения текста одной страницы
    PDF_PROFILE,  # Узнаём активный профиль обработки
    PDF_SCAN_SAMPLE,  # Управляем выборочным сканированием
    PDF_SCAN_STRIDE,  # Получаем шаг выборки страниц
    PDF_STOP_AFTER_NO_HITS,  # Ограничиваем количество подряд пустых страниц
    PARSE_COLLECT_ALL,  # Решаем, нужно ли собирать все адреса без раннего выхода
    PROGRESS_UPDATE_EVERY_PAGES,  # Контролируем частоту обновления прогресса по страницам
    PROGRESS_UPDATE_MIN_SEC,  # Учитываем минимальный интервал обновления прогресса в секундах
)
from emailbot.cancel_token import is_cancelled  # Обеспечиваем реакцию на внешнюю отмену
from emailbot.utils.run_with_timeout import run_with_timeout  # Выполняем получение текста с тайм-аутом
from emailbot.utils.text_preprocess import normalize_for_email  # Приводим текст к форме для поиска адресов
from emailbot.parsing.extract_from_text import emails_from_text  # Выделяем e-mail адреса из текста
from emailbot.pdfminer_page import (  # Подключаем pdfminer-фолбэки
    count_pages_fast,  # Быстро оцениваем количество страниц
    extract_page_text,  # Извлекаем текст страницы при отсутствии PyMuPDF
)


def _effective_target(target: Optional[int]) -> Optional[int]:
    """Return an adjusted target accounting for global configuration knobs."""

    if PARSE_COLLECT_ALL:  # Если требуется собрать все адреса
        return None  # Целевой порог отключаем
    if target is not None and target > 0:  # Если передан явный целевой порог
        return target  # Используем пользовательское значение
    if PDF_FOUND_TARGET > 0:  # Иначе опираемся на конфигурацию
        return int(PDF_FOUND_TARGET)  # Возвращаем глобальный порог
    return None  # При нулевом пороге отключаем ранний выход


def _hard_page_cap(total: int) -> int:
    """Compute the maximum number of pages allowed for fast scanning."""

    profile_limit = 0  # Инициализируем профильный предел
    if PDF_PROFILE == "fast" and PDF_FAST_LIMIT_PAGES > 0:  # Проверяем быстрый профиль
        profile_limit = int(PDF_FAST_LIMIT_PAGES)  # Фиксируем лимит из настроек
    global_limit = int(PDF_MAX_PAGES or 0)  # Берём глобальный предел
    candidates = [value for value in (total, profile_limit, global_limit) if value > 0]  # Составляем список ограничений
    if not candidates:  # Если ограничений нет
        return 0  # Возвращаем ноль для обозначения отсутствия лимита
    return min(candidates)  # Выбираем минимальный лимит как фактический


def _notify_total(progress: Optional[object], total: int) -> None:
    """Safely deliver total page count to the progress tracker."""

    if not progress:  # Проверяем наличие объекта прогресса
        return  # Если прогресс не нужен, сразу выходим
    try:  # Передаём значение в защищённом блоке
        progress.set_total(total)  # Обновляем ожидаемое количество страниц
    except Exception:  # Игнорируем ошибки обновления прогресса
        pass  # Оставляем прогресс без изменений


def _update_progress(progress: Optional[object], found: Set[str], pages: int) -> None:
    """Update progress counters with defensive guards."""

    if not progress:  # Убеждаемся, что прогресс требуется
        return  # Выходим, если наблюдатель не передан
    try:  # Оборачиваем обновление в try, чтобы не ронять обработку
        progress.inc_pages(pages)  # Инкрементируем количество обработанных страниц
    except Exception:  # В случае ошибки инкремента
        pass  # Пропускаем обновление
    try:  # Обновляем число найденных адресов
        progress.set_found(len(found))  # Фиксируем текущее количество адресов
    except Exception:  # Игнорируем ошибки установки значения
        pass  # Пропускаем обновление


def _yield_indices(total: int) -> list[int]:
    """Return list of page indices respecting sampling settings."""

    if total <= 0:  # Если общее количество страниц неизвестно
        return [0]  # Возвращаем хотя бы одну страницу для обработки
    indices: list[int] = []  # Создаём список индексов
    stride = max(1, int(PDF_SCAN_STRIDE))  # Гарантируем положительный шаг выборки
    if PDF_SCAN_SAMPLE:  # Проверяем, нужно ли выборочное сканирование
        indices.extend(range(0, total, stride))  # Добавляем выборку страниц с указанным шагом
    if not indices or indices[-1] != total - 1:  # Убеждаемся, что последняя страница тоже будет рассмотрена
        indices.extend(range(total))  # Добавляем последовательный проход по всем страницам
    unique = []  # Готовим список уникальных индексов
    seen = set()  # Создаём множество уже добавленных индексов
    for idx in indices:  # Перебираем все кандидаты
        if idx in seen:  # Пропускаем дубли
            continue  # Переходим к следующему индексу
        seen.add(idx)  # Отмечаем индекс как использованный
        if idx < total:  # Убеждаемся, что индекс в пределах документа
            unique.append(idx)  # Добавляем индекс в итоговый список
    return unique  # Возвращаем последовательность без дубликатов


def _page_text_via_doc(doc, index: int) -> str:
    """Extract text from a PyMuPDF document page with defensive fallbacks."""

    try:  # Пытаемся получить страницу из документа
        page = doc[index]  # Извлекаем страницу по индексу
    except Exception:  # При ошибках доступа к странице
        return ""  # Возвращаем пустую строку
    try:  # Пытаемся извлечь текст страницы
        return page.get_text("text") or ""  # Возвращаем текст, нормализуя None
    except Exception:  # В случае ошибок извлечения
        return ""  # Возвращаем пустую строку


def _page_text_via_pdfminer(pdf_path: Path, index: int) -> str:
    """Return page text using the pdfminer fallback helpers."""

    return extract_page_text(pdf_path, index)  # Делегируем извлечение pdfminer-функции


def extract_emails_from_pdf_fast_core(
    doc,  # Обработчик PDF из PyMuPDF или None
    *,
    limit_pages: Optional[int] = None,  # Необязательный жёсткий лимит страниц
    target: Optional[int] = None,  # Пользовательский порог количества адресов
    progress: Optional[object] = None,  # Объект обновления прогресса
    pdf_path: Optional[Path] = None,  # Путь к PDF для pdfminer-фолбэка
) -> Set[str]:  # Возвращаем множество адресов
    """Iterate over pages in ``doc`` (or via pdfminer) and collect e-mails quickly."""

    found: Set[str] = set()  # Инициализируем множество найденных адресов
    try:  # Пытаемся определить количество страниц из документа
        total_pages = len(doc) if doc is not None else 0  # Получаем длину документа
    except Exception:  # При ошибке доступа к длине
        total_pages = 0  # Сбрасываем количество страниц в ноль
    if total_pages <= 0 and pdf_path is not None:  # Если PyMuPDF не дал ответ, пробуем pdfminer
        total_pages = count_pages_fast(pdf_path)  # Запрашиваем приблизительное число страниц
    if total_pages <= 0:  # Если по-прежнему неизвестно количество страниц
        total_pages = 1  # Обрабатываем хотя бы одну страницу
    if limit_pages is not None and limit_pages > 0:  # Учитываем явный лимит страниц
        total_pages = min(total_pages, int(limit_pages))  # Ограничиваем общее число страниц
    cap = _hard_page_cap(total_pages)  # Рассчитываем фактический предел страниц
    if cap > 0:  # Если предел установлен
        total_pages = min(total_pages, cap)  # Применяем ограничение
    _notify_total(progress, total_pages)  # Сообщаем общий прогресс
    indices = _yield_indices(total_pages)  # Получаем последовательность индексов для обработки
    effective_target = _effective_target(target)  # Вычисляем целевой порог адресов
    nohit_run = 0  # Счётчик подряд идущих пустых страниц
    last_progress = 0.0  # Метка времени последнего обновления прогресса
    update_every = max(1, int(PROGRESS_UPDATE_EVERY_PAGES))  # Период обновления по страницам
    update_min_interval = max(0.5, float(PROGRESS_UPDATE_MIN_SEC))  # Минимальный интервал в секундах

    for processed, index in enumerate(indices, start=1):  # Перебираем страницы с порядковым номером
        if is_cancelled():  # Проверяем глобальный токен отмены
            break  # Прерываем обработку при отмене
        if PDF_STOP_AFTER_NO_HITS > 0 and nohit_run >= int(PDF_STOP_AFTER_NO_HITS):  # Ограничиваем серию пустых страниц
            break  # Останавливаемся, если слишком долго нет результатов
        def _get_text() -> str:  # Объявляем функцию получения текста для run_with_timeout
            if doc is not None:  # Когда доступен PyMuPDF-документ
                return _page_text_via_doc(doc, index)  # Получаем текст через PyMuPDF
            if pdf_path is not None:  # Если доступен путь к файлу
                return _page_text_via_pdfminer(pdf_path, index)  # Используем pdfminer-фолбэк
            return ""  # В остальных случаях возвращаем пустую строку
        try:  # Выполняем получение текста с тайм-аутом
            raw_text = run_with_timeout(_get_text, PDF_PAGE_TIMEOUT_SEC)
        except Exception:  # При превышении тайм-аута или других ошибках
            raw_text = ""  # Считаем страницу пустой
        normalized = normalize_for_email(raw_text or "")  # Подготавливаем текст к поиску адресов
        if normalized:  # Проверяем, содержит ли страница полезный текст
            before = len(found)  # Запоминаем количество адресов до обработки
            found |= emails_from_text(normalized)  # Добавляем найденные адреса
            if len(found) == before:  # Оцениваем, появились ли новые адреса
                nohit_run += 1  # Увеличиваем счётчик пустых страниц
            else:  # Если адреса найдены
                nohit_run = 0  # Сбрасываем счётчик пустых страниц
        else:  # При полном отсутствии текста
            nohit_run += 1  # Считаем страницу пустой
        _update_progress(progress, found, 1)  # Обновляем прогресс обработкой одной страницы
        now = time.time()  # Получаем текущий момент времени
        if progress and processed % update_every == 0 and now - last_progress >= update_min_interval:  # Проверяем частоту обновлений
            try:  # Обновляем фазу прогресса
                progress.set_phase("PDF")  # Сообщаем, что всё ещё парсим PDF
            except Exception:  # Игнорируем ошибки установки фазы
                pass  # Пропускаем обновление
            last_progress = now  # Запоминаем момент обновления
        if effective_target is not None and len(found) >= effective_target:  # Проверяем достижение целевого количества адресов
            break  # Завершаем ранний выход при достижении порога

    return found  # Возвращаем множество найденных адресов


def extract_emails_fitz(
    pdf_path: Path,  # Путь к PDF-файлу
    progress: Optional[object] = None,  # Объект прогресса
) -> Set[str]:  # Возвращаем множество адресов
    """Extract e-mails via PyMuPDF if available, falling back gracefully."""

    try:  # Пытаемся импортировать PyMuPDF
        import fitz  # type: ignore  # noqa: F401  # Загружаем модуль fitz
    except Exception:  # Если PyMuPDF недоступен
        return set()  # Возвращаем пустое множество адресов

    try:  # Открываем документ PyMuPDF
        doc = fitz.open(str(pdf_path))  # type: ignore[attr-defined]
    except Exception:  # При ошибке открытия документа
        doc = None  # Переходим в режим pdfminer

    try:  # Проводим основную обработку
        return extract_emails_from_pdf_fast_core(
            doc,  # Передаём документ или None
            progress=progress,  # Прокидываем объект прогресса
            pdf_path=pdf_path,  # Сообщаем путь для pdfminer-фолбэка
        )
    finally:  # Всегда закрываем документ при наличии
        if doc is not None:  # Проверяем, что документ открыт
            try:  # Пытаемся закрыть документ
                doc.close()  # Освобождаем ресурсы
            except Exception:  # Игнорируем ошибки закрытия
                pass  # Не допускаем аварийного завершения
