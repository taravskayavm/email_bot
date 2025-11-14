"""Мост между текстовыми отчётами и просмотрщиком примеров в боте."""  # Объясняем назначение модуля

from __future__ import annotations  # Гарантируем корректную работу аннотаций в ранних версиях Python

from typing import Any, Iterable, Sequence  # Импортируем типы для повышения читаемости и статпроверок

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
    )

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
    )


async def send_parse_report(
    message: Message,
    report_text: str,
    summary: dict[str, Any],
    source_id: str,
) -> None:
    """Отправить отчёт после парсинга и очистить текст от технических строк."""  # Описываем назначение функции

    cleaned_text = format_parse_report(report_text)  # Удаляем строки про подозрительные адреса и дубликаты

    _store_examples(
        message,
        source_kind="parse",
        source_id=source_id,
        cooldown=summary.get("cooldown_list"),
        foreign=summary.get("foreign_list"),
        summary=summary,
    )  # Сохраняем контекст примеров по результатам парсинга

    await message.answer(  # Отправляем пользователю очищенный отчёт
        cleaned_text,  # Передаём текст без вспомогательных технических строк
        reply_markup=build_examples_entry_kb(),  # Добавляем кнопку для просмотра примеров
    )

