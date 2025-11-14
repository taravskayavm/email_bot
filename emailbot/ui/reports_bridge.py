"""Мост между текстовыми отчётами и просмотрщиком примеров в боте."""  # Объясняем назначение модуля

from __future__ import annotations  # Гарантируем корректную работу аннотаций в ранних версиях Python

from typing import Any, Iterable, Sequence  # Импортируем типы для повышения читаемости и статпроверок

import re  # Используем регулярные выражения для парсинга примеров из текста

from aiogram.types import Message  # Работаем с сообщениями Telegram через aiogram

from emailbot.bot.handlers.examples import store_examples_context  # Сохраняем контекст примеров в кэше
from emailbot.bot.keyboards import build_examples_entry_kb  # Строим клавиатуру «Показать примеры»
from emailbot.state.example_cache import ExamplesContext, ListPager  # Используем контекст и пагинаторы примеров
from emailbot.ui.messages import format_parse_report  # Чистим текст отчёта после парсинга от лишних строк


def _as_str(value: object) -> str:
    """Безопасно преобразовать объект к строке с обрезкой пробелов."""  # Объясняем вспомогательную функцию

    return str(value or "").strip()  # Приводим значение к строке и убираем пробелы по краям


def _normalize_cooldown_entries(raw: Iterable[object] | None) -> list[tuple[str, str]]:
    """Подготовить список пар «адрес — дата последней отправки» для пагинатора."""  # Описываем цель функции

    entries: list[tuple[str, str]] = []  # Создаём итоговый список пар
    for item in raw or []:  # Перебираем исходную коллекцию, допускаем None
        email = ""  # Инициализируем строку e-mail
        last_date = ""  # Инициализируем строку даты
        if isinstance(item, dict):  # Обрабатываем вариант, когда данные переданы словарём
            email = _as_str(item.get("email") or item.get("address"))  # Извлекаем ключи с адресом
            last_date = _as_str(item.get("last_date") or item.get("last"))  # Извлекаем дату последнего письма
        elif isinstance(item, Sequence) and not isinstance(item, (str, bytes)):  # Обрабатываем последовательности
            if item:  # Проверяем, что в последовательности есть элементы
                email = _as_str(item[0])  # Берём первый элемент как адрес
                if len(item) > 1:  # Проверяем наличие даты в последовательности
                    last_date = _as_str(item[1])  # Берём второй элемент как дату
        else:  # Попадаем сюда, если передан одиночный объект
            email = _as_str(item)  # Считаем объект строкой с адресом
        if not email:  # Пропускаем записи без адреса
            continue  # Переходим к следующему элементу
        entries.append((email, last_date))  # Добавляем нормализованную пару в список
    return entries  # Возвращаем подготовленный список пар


def _normalize_foreign_entries(raw: Iterable[object] | None) -> list[str]:
    """Подготовить список иностранных адресов для пагинатора."""  # Объясняем назначение функции

    entries: list[str] = []  # Создаём итоговый список строк
    for item in raw or []:  # Перебираем элементы исходной коллекции
        email = _as_str(item)  # Приводим значение к строке адреса
        if not email:  # Пропускаем пустые значения
            continue  # Переходим к следующему элементу
        entries.append(email)  # Добавляем нормализованный адрес
    return entries  # Возвращаем итоговый список иностранных адресов


def _extract_total(summary: dict[str, Any], *keys: str, fallback: int) -> int:
    """Вернуть числовое значение по первым доступным ключам либо fallback."""  # Описываем цель функции

    for key in keys:  # Перебираем ключи в порядке приоритета
        if key not in summary:  # Пропускаем отсутствующие ключи
            continue  # Переходим к следующему ключу
        try:  # Пробуем привести значение к целому числу
            return int(summary[key])  # Возвращаем успешно преобразованное значение
        except (TypeError, ValueError):  # Если преобразование не удалось
            continue  # Ищем значение по следующему ключу
    return fallback  # Возвращаем запасной вариант, если ничего не найдено


def _store_examples(
    message: Message,
    *,
    source_kind: str,
    source_id: str,
    cooldown: Iterable[object] | None,
    foreign: Iterable[object] | None,
    summary: dict[str, Any],
) -> None:
    """Собрать контекст примеров и сохранить его для указанного чата."""  # Объясняем назначение функции

    chat = getattr(message, "chat", None)  # Получаем объект чата из сообщения
    chat_id = getattr(chat, "id", None)  # Извлекаем идентификатор чата, если он присутствует
    if chat_id is None:  # Проверяем, удалось ли получить идентификатор
        return  # Завершаем работу, если контекст сохранить некуда

    cooldown_entries = _normalize_cooldown_entries(cooldown)  # Нормализуем данные по кулдауну
    foreign_entries = _normalize_foreign_entries(foreign)  # Нормализуем список иностранных адресов

    cooldown_total = _extract_total(
        summary,
        "cooldown_total",
        "skipped_cooldown",
        fallback=len(cooldown_entries),
    )  # Определяем итоговое количество адресов под кулдауном
    foreign_total = _extract_total(
        summary,
        "foreign_total",
        "foreign_deferred",
        fallback=len(foreign_entries),
    )  # Определяем итоговое количество иностранных адресов

    context = ExamplesContext(  # Формируем структуру контекста примеров
        source_kind=source_kind,  # Указываем источник данных для UI
        source_id=source_id,  # Сохраняем идентификатор источника (например, run_id)
        cooldown_total=cooldown_total,  # Запоминаем количество адресов под кулдауном
        foreign_total=foreign_total,  # Запоминаем количество иностранных адресов
        cooldown_pager=ListPager(cooldown_entries),  # Создаём пагинатор по кулдауну
        foreign_pager=ListPager(foreign_entries),  # Создаём пагинатор по иностранным адресам
    )  # Завершаем создание структуры контекста примеров

    store_examples_context(chat_id, context)  # Сохраняем готовый контекст в кэш роутера


async def send_post_send_report(
    message: Message,
    report_text: str,
    summary: dict[str, Any],
    source_id: str,
) -> None:
    """Отправить отчёт об отправке и подготовить примеры для просмотра."""  # Объясняем назначение функции

    _store_examples(
        message,
        source_kind="send",
        source_id=source_id,
        cooldown=summary.get("cooldown_list"),
        foreign=summary.get("foreign_deferred_list") or summary.get("foreign_list"),
        summary=summary,
    )  # Подготавливаем и сохраняем контекст примеров для рассылки

    await message.answer(  # Отправляем финальный отчёт пользователю
        report_text,  # Передаём сформированный текст отчёта
        reply_markup=build_examples_entry_kb(),  # Добавляем кнопку для показа примеров
    )  # Завершаем вызов отправки финального отчёта с клавиатурой


async def send_parse_report(
    message: Message,
    report_text: str,
    summary: dict[str, Any],
    source_id: str,
) -> None:
    """Отправить отчёт после парсинга и дополнительно извлечь примеры из текста при их отсутствии."""  # Описываем назначение функции

    summary = dict(summary or {})  # Создаём копию summary, чтобы безопасно пополнять её извлечёнными данными
    cleaned_text = format_parse_report(report_text)  # Удаляем строки про подозрительные адреса и дубликаты

    cooldown_list = summary.get("cooldown_list")  # Читаем список примеров адресов под кулдауном из summary
    foreign_list = summary.get("foreign_list")  # Читаем список иностранных адресов из summary

    extracted_cooldown: list[tuple[str, str]] = []  # Готовим список для найденных в тексте пар «адрес — дата»
    extracted_foreign: list[str] = []  # Готовим список для найденных в тексте иностранных адресов

    if not cooldown_list or not foreign_list:  # Запускаем авторазбор, если каких-то списков не хватает
        cooldown_match = re.search(  # Ищем блок про кулдаун по шаблону "Примеры 180 дней"
            r"Примеры\s*180\s*дней\s*:\s*(.+?)(?:\n\s*\n|$)",
            cleaned_text,
            flags=re.IGNORECASE | re.DOTALL,
        )  # Завершаем передачу аргументов поиску блока кулдауна
        if cooldown_match:  # Проверяем, найден ли блок
            cooldown_block = cooldown_match.group(1)  # Получаем содержимое блока с примерами
            for entry in re.finditer(  # Перебираем все совпадения адресов внутри блока
                r"[•\-]\s*([\w.\-+%]+@[\w.\-]+)\s*[—\-]\s*([0-9]{4}-[0-9]{2}-[0-9]{2})",
                cooldown_block,
            ):  # Завершаем перечисление аргументов для поиска адресов кулдауна
                extracted_cooldown.append((entry.group(1), entry.group(2)))  # Сохраняем найденную пару адреса и даты
        if not cooldown_list and extracted_cooldown:  # Используем извлечённые данные, если изначально список пустой
            cooldown_list = extracted_cooldown  # Подставляем новый список в переменную
            summary["cooldown_list"] = cooldown_list  # Сохраняем примеры в summary для единообразия
            summary.setdefault("cooldown_total", len(extracted_cooldown))  # Обновляем счётчик адресов под кулдауном

        foreign_match = re.search(  # Ищем блок про иностранные адреса
            r"Примеры\s*иностранных\s*:\s*(.+?)(?:\n\s*\n|$)",
            cleaned_text,
            flags=re.IGNORECASE | re.DOTALL,
        )  # Завершаем передачу аргументов поиску блока иностранных адресов
        if foreign_match:  # Проверяем, найден ли блок
            foreign_block = foreign_match.group(1)  # Получаем текст блока с иностранными адресами
            for entry in re.finditer(  # Перебираем все адреса внутри блока
                r"[•\-]\s*([\w.\-+%]+@[\w.\-]+)",
                foreign_block,
            ):  # Завершаем перечисление аргументов для поиска иностранных адресов
                extracted_foreign.append(entry.group(1))  # Добавляем найденный адрес в список
        if not extracted_foreign:  # Если явного блока нет, пытаемся извлечь адреса из строки с доменами
            foreign_line_match = re.search(  # Ищем строку "Иностранные домены"
                r"Иностранные\s*домены[^:]*:\s*(.+?)(?:\n|$)",
                cleaned_text,
                flags=re.IGNORECASE,
            )  # Завершаем передачу аргументов для поиска строки с доменами
            if foreign_line_match:  # Проверяем, удалось ли найти строку
                foreign_line = foreign_line_match.group(1)  # Извлекаем содержимое строки с доменами
                for entry in re.finditer(  # Перебираем адреса в найденной строке
                    r"([\w.\-+%]+@[\w.\-]+)",
                    foreign_line,
                ):  # Завершаем перечисление аргументов при поиске адресов в строке
                    extracted_foreign.append(entry.group(1))  # Добавляем найденный адрес
        if not foreign_list and extracted_foreign:  # Используем найденные адреса, если исходный список пустой
            foreign_list = extracted_foreign  # Подставляем новый список в переменную
            summary["foreign_list"] = foreign_list  # Сохраняем примеры в summary для повторного использования
            summary.setdefault("foreign_total", len(extracted_foreign))  # Обновляем счётчик иностранных адресов

    _store_examples(
        message,
        source_kind="parse",
        source_id=source_id,
        cooldown=cooldown_list,
        foreign=foreign_list,
        summary=summary,
    )  # Сохраняем контекст примеров по результатам парсинга

    await message.answer(  # Отправляем пользователю очищенный отчёт
        cleaned_text,  # Передаём текст без вспомогательных технических строк
        reply_markup=build_examples_entry_kb(),  # Добавляем кнопку для просмотра примеров
    )  # Завершаем отправку очищенного отчёта пользователю

