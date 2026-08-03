from __future__ import annotations  # Подключаем будущие аннотации для совместимости с Python 3.10

from datetime import date  # Импортируем date для вывода границ периодов в отчётах
from typing import Iterable, Mapping, TYPE_CHECKING, List, Tuple  # Импортируем типы для работы со словарями, списками и коллекциями

if TYPE_CHECKING:  # pragma: no cover - ветка для подсказок типов  # Выполняем импорт только для статического анализа
    from emailbot.reporting import PeriodStats  # Подключаем структуру статистики отправок для аннотаций

# ВАЖНО: не делаем жёсткий импорт на уровне модуля — возможны циклические импорты
_HAVE_COUNT_BLOCKED = True  # Флаг успешного доступа к функции подсчёта блокировок
try:  # Пытаемся импортировать функцию подсчёта блокировок сразу
    from emailbot.reporting import count_blocked  # type: ignore  # Пытаемся подгрузить функцию подсчёта блокировок
except Exception:  # pragma: no cover - защитный путь  # При любом исключении оставляем заглушку
    count_blocked = None  # type: ignore[assignment]  # Храним None, чтобы позднее попробовать импорт снова
    _HAVE_COUNT_BLOCKED = False  # Фиксируем, что импорт пока не состоялся


def _as_int(value: object, default: int = 0) -> int:
    """Безопасное приведение к int с дефолтом."""
    try:  # Пробуем выполнить преобразование типов
        return int(value)  # Пробуем привести значение к целому числу
    except Exception:  # Возвращаем запасное значение при любом сбое
        return default  # При ошибке возвращаем запасное значение


# Старый «приятный» стиль сообщений под Telegram (эмодзи + плотные подпункты).
# Никакого HTML – чистый текст/Markdown-safe (aiogram parse_mode="HTML"/"MarkdownV2" на твой выбор).
def format_parse_summary(s: Mapping[str, object], examples: Iterable[str] = ()) -> str:
    """
    Ожидаемые ключи s:
      total_found, to_send,
      cooldown_180d, foreign_domain,
      invalid, technical, blocked, blocked_after_parse

    Ключи, которые больше не используются в тексте, но могут приходить:
      suspicious, footnote_dupes_removed
    """
    lines: list[str] = []  # Список строк, из которых соберём сообщение
    lines.append("✅ Анализ завершён.")  # Сообщаем о завершении анализа
    lines.append(f"Всего найдено: {s.get('total_found', 0)}")
    lines.append(f"🌍 Иностранные домены: {s.get('foreign_domain', 0)}")

    invalid = _as_int(s.get("invalid", 0), 0)
    if invalid > 0:
        lines.append(f"❌ Некорректные адреса: {invalid}")

    technical = _as_int(s.get("technical", 0), 0)
    if technical > 0:
        lines.append(f"🛠 Технические адреса: {technical}")

    blocked_value = s.get("blocked_after_parse")
    if blocked_value is None:
        blocked_value = s.get("blocked", 0)
    blocked = _as_int(blocked_value, 0)
    if blocked > 0:
        lines.append(f"🚫 В стоп-листе: {blocked}")

    lines.append(f"⏳ Под кулдауном (180 дней): {s.get('cooldown_180d', 0)}")
    lines.append(f"📦 К отправке: {s.get('to_send', 0)}")

    lines.append("")  # Разделяем основные цифры и примеры пустой строкой

    def _append_examples(title: str, key: str, limit: int | None = None) -> bool:
        """Добавить в отчёт примеры по ключу s[key], опционально ограничив число."""
        values = s.get(key)  # Достаём значения по ключу из входного словаря
        if not values:  # Проверяем наличие значений в отчёте
            return False  # Ничего не добавляем, если значения отсутствуют

        if isinstance(values, str):  # Обрабатываем строковый вариант значения
            iterable = [values]  # Строку упаковываем в список для единообразной обработки
        else:  # Для остальных вариантов пытаемся интерпретировать значение как коллекцию
            try:  # Пробуем получить итерируемую коллекцию
                iterable = list(values)  # Пробуем превратить коллекцию в список
            except TypeError:  # Отлавливаем случай неитерируемых значений
                iterable = [values]  # Если значение не итерируемо, оборачиваем его в список

        samples_raw = [str(item).strip() for item in iterable if str(item).strip()]  # Нормализуем примеры и убираем пустые
        if not samples_raw:  # Проверяем, остались ли примеры после фильтрации
            return False  # Возвращаем False, если после фильтрации ничего не осталось

        if limit is not None and limit > 0:  # Ограничиваем размер списка примеров при необходимости
            samples_raw = samples_raw[:limit]  # Ограничиваем количество примеров, если задан лимит

        samples = [item[:80] for item in samples_raw]  # Усечём каждый пример до 80 символов для компактности
        if not samples:  # Если после усечения список пуст, завершаем без добавления
            return False  # Защита от пустых списков на всякий случай

        lines.append(title)  # Добавляем заголовок блока с примерами
        for sample in samples:  # Перебираем подготовленные примеры
            lines.append(f" • {sample}")  # Перечисляем каждую запись в отдельной строке
        return True  # Сообщаем вызывающему коду, что примеры были добавлены

    appended = False  # Флаг, были ли добавлены примеры
    appended |= _append_examples("❗ Примеры некорректных доменов:", "invalid_tld_examples")  # Примеры неправильных доменов
    appended |= _append_examples("❗ Синтаксические отказы:", "syntax_fail_examples")  # Примеры синтаксических ошибок
    appended |= _append_examples("ℹ️ Исправлены гомоглифы:", "confusable_fixed_examples")  # Примеры исправленных гомоглифов
    if appended:  # Добавляем разделитель только если примеры были добавлены
        lines.append("")  # Добавляем пустую строку, если примеры присутствуют

    return "\n".join(lines)  # Собираем итоговое сообщение


def format_parse_report(raw_text: str) -> str:
    """Удалить из текстового отчёта строки про подозрительные и сноски."""  # Описываем назначение функции

    if not raw_text:  # Проверяем, передан ли текст отчёта
        return ""  # Возвращаем пустую строку, если исходные данные отсутствуют

    cleaned_lines: list[str] = []  # Подготавливаем контейнер для отфильтрованных строк
    for line in raw_text.splitlines():  # Перебираем каждую строку исходного отчёта
        if "Подозрительные" in line:  # Отбрасываем строки, описывающие подозрительные адреса
            continue  # Переходим к следующей строке, не добавляя текущую
        if "Возможные сносочные дубликаты удалены" in line:  # Убираем строки про сносочные дубликаты
            continue  # Пропускаем текущую строку, чтобы не выводить её пользователю
        cleaned_lines.append(line)  # Добавляем полезную строку в итоговый список

    return "\n".join(cleaned_lines)  # Склеиваем оставшиеся строки обратно в текст


def format_direction_selected(name_ru: str, code: str | None = None) -> str:
    """Сформировать уведомление о выборе шаблона."""
    if code:  # Проверяем, выбран ли именованный шаблон
        return f"✅ Выбран шаблон: «{name_ru}» ({code})"  # Возвращаем сообщение с кодом шаблона
    return f"✅ Выбран шаблон: «{name_ru}»"  # Возвращаем сообщение без кода


def format_dispatch_preview(stats: Mapping[str, int], xlsx_name: str) -> str:
    """
    Ожидаемые ключи:
      ready_to_send, deferred_180d, in_blacklists, need_review
    """
    return (  # Собираем краткое превью рассылки
        f"📄 {xlsx_name}\n"  # Добавляем название файла с выгрузкой
        f"🚀 Готово к отправке: {stats.get('ready_to_send', 0)}\n"  # Показываем число писем к отправке
        f"⏳ Отложено по правилу 180 дн.: {stats.get('deferred_180d', 0)}\n"  # Сколько писем попало под кулдаун
        f"🚫 В стоп-листе: {stats.get('in_blacklists', 0)}\n"  # Количество адресов в стоп-листах
        f"🔍 Требует проверки: {stats.get('need_review', 0)}"  # Сколько адресов требует ручной проверки
    )


def format_dispatch_start(
    planned: int,
    unique: int,
    to_send: int,
    *,
    deferred: int = 0,
    suppressed: int = 0,
    foreign: int = 0,
    duplicates: int = 0,
    limited_from: int | None = None,
) -> str:
    """Сформировать стартовое сообщение о рассылке."""
    lines = [  # Формируем базовый набор строк для сообщения о старте
        "✉️ Рассылка начата.",  # Базовая строка о старте рассылки
        f"Запрошено: {planned}",  # Сколько писем было запрошено
        f"Уникальных: {unique}",  # Сколько уникальных адресов найдено
    ]
    if limited_from is not None and limited_from > to_send:  # Проверяем наличие ограничения по лимиту
        lines.append(
            f"К отправке (после фильтров и лимитов): {to_send} из {limited_from}"
        )  # Показываем ограничение по лимиту
    else:  # Если дополнительного лимита нет, используем значение после фильтров
        lines.append(f"К отправке (после фильтров): {to_send}")  # Сообщаем количество после фильтров
    if deferred:  # Добавляем строку про отложенные письма
        lines.append(f"Отложено по правилу 180 дней: {deferred}")  # Добавляем статистику по кулдауну
    if suppressed:  # Учитываем письма, исключённые супрессией
        lines.append(f"Исключено (супресс/стоп-лист): {suppressed}")  # Сколько адресов исключено
    if foreign:  # Сообщаем об иностранных доменах
        lines.append(f"Отложено (иностранные домены): {foreign}")  # Сколько адресов убрано как иностранные
    if duplicates:  # Указываем количество найденных дубликатов
        lines.append(f"Дубликаты в пачке: {duplicates}")  # Сколько дубликатов найдено
    return "\n".join(lines)  # Возвращаем собранные строки


def format_dispatch_result(
    total: int,
    sent: int,
    cooldown_skipped: int,
    blocked: int,
    duplicates: int = 0,
    *,
    aborted: bool = False,
) -> str:
    """Итоговая сводка для старого (legacy) сценария отправки."""
    lines: list[str] = [  # Формируем базовый набор строк итоговой сводки
        "📨 Рассылка завершена.",  # Сообщаем об окончании процесса
        f"📊 В очереди было: {total}",  # Количество писем в очереди
        f"✅ Отправлено: {sent}",  # Сколько писем успешно отправлено
        f"⏳ Пропущены (по правилу «180 дней»): {cooldown_skipped}",  # Сколько писем пропущено из-за кулдауна
        f"🚫 В стоп-листе: {blocked}",  # Сколько адресов попало в стоп-лист
    ]
    if duplicates:  # Добавляем информацию о найденных дубликатах
        lines.append(f"♻️ Дубликаты за 24 ч: {duplicates}")  # Отмечаем найденные дубликаты
    if aborted:  # Сообщаем, если процесс рассылки был остановлен
        lines.append("⚠️ Процесс был остановлен по запросу.")  # Добавляем предупреждение, если рассылка прервана
    return "\n".join(lines)  # Возвращаем сформированное сообщение


def render_dispatch_summary(
    *,
    planned: int,
    sent: int,
    skipped_cooldown: int,
    skipped_initial: int,
    errors: int,
    audit_path: str | None,
    planned_emails: Iterable[str] | None = None,
    raw_emails: Iterable[str] | None = None,
    blocked_count: int | None = None,
) -> str:
    """Итоговая сводка для aiogram-бота."""
    total_skipped = max(skipped_cooldown, skipped_initial)  # Берём максимум пропусков для отчёта

    final_blocked = blocked_count  # Пытаемся использовать переданное число блокировок
    if final_blocked is None:  # При отсутствии явного значения пересчитываем блокировки
        blocked_source = planned_emails or raw_emails or []  # Определяем источник адресов для пересчёта
        final_blocked = 0  # Инициализируем счётчик блокировок
        try:  # Пытаемся пересчитать блокировки по исходным адресам
            global count_blocked, _HAVE_COUNT_BLOCKED  # Указываем на использование глобальных переменных
            if not _HAVE_COUNT_BLOCKED:  # При первом обращении доимпортируем функцию
                from emailbot.reporting import (  # type: ignore  # Переимпортируем функцию при необходимости
                    count_blocked as _count_blocked  # Забираем функцию и сохраняем локальный алиас
                )

                count_blocked = _count_blocked  # type: ignore[assignment]  # Сохраняем функцию в глобальной переменной
                _HAVE_COUNT_BLOCKED = True  # Отмечаем, что функция стала доступна
            if callable(count_blocked):  # Убеждаемся, что функция доступна для вызова
                final_blocked = count_blocked(blocked_source)  # type: ignore[arg-type]  # Пересчитываем количество блокировок
        except Exception:  # Любые ошибки пересчёта приводят к безопасному нулю
            final_blocked = 0  # На случай ошибки возвращаем безопасное значение

    audit_suffix = f"\n\n🧾 Аудит: {audit_path}" if audit_path else ""  # Добавляем путь до аудита при наличии

    return (
        "📨 Рассылка завершена.\n"  # Сообщаем о завершении
        f"📊 В очереди было: {planned}\n"  # Показываем исходное количество адресов
        f"✅ Отправлено: {sent}\n"  # Отображаем успешные отправки
        f"⏳ Пропущены (по правилу «180 дней»): {total_skipped}\n"  # Показываем пропуски
        f"🚫 В стоп-листе: {final_blocked}\n"  # Фиксируем количество стопов
        f"❌ Ошибок при отправке: {errors}"  # Отображаем количество ошибок
        f"{audit_suffix}"  # Добавляем сведения об аудите
    )


def format_error_details(details: Iterable[str]) -> str:
    """Сейчас не выводим скрытую сводку ошибок (оставляем поведение по умолчанию)."""
    return ""  # Возвращаем пустую строку, чтобы не отправлять скрытые подробности


def _format_period_header(period: str, date_start: date, date_end: date) -> str:
    """Сформировать согласованный заголовок детального отчёта."""

    if period == "day":
        return (
            "📊 Отчёт по отправленным письмам за день "
            f"{date_start.strftime('%d.%m.%Y')}"
        )

    names = {
        "week": "неделю",
        "month": "месяц",
        "year": "год",
    }
    label = names.get(period, "период")
    interval = (
        f"{date_start.strftime('%d.%m.%Y')}–{date_end.strftime('%d.%m.%Y')}"
    )
    return f"📊 Отчёт по отправленным письмам за {label} {interval}"


def _format_count(value: int) -> str:
    """Format integer counts with spaces as thousands separators."""

    return f"{value:,}".replace(",", " ")


def format_send_stats_by_direction(stats: PeriodStats) -> str:
    """Отформатировать статистику отправок по направлениям за период."""

    lines = [_format_period_header(stats.period, stats.date_start, stats.date_end), ""]
    if stats.directions:
        for direction in stats.directions:
            if direction.success > 0:
                lines.append(f"{direction.title} — {_format_count(direction.success)}")
    else:
        lines.append("За выбранный период отправок не найдено.")

    lines.append("")
    lines.append(f"Всего отправлено: {_format_count(stats.total_success)}")
    return "\n".join(lines)


def format_examples_block(
    cooldown_total: int,  # Количество адресов, попавших под ограничение 180 дней
    cooldown_triplet: List[Tuple[str, str]],  # Список примеров для кулдауна
    foreign_total: int,  # Количество адресов с иностранными доменами
    foreign_triplet: List[str],  # Список примеров иностранных адресов
    *,
    foreign_title_suffix: str = "",  # Дополнительное уточнение к заголовку иностранных адресов
) -> str:
    """Собрать текстовый блок с примерами кулдауна и иностранных адресов."""

    lines: list[str] = []  # Подготавливаем список строк для итогового сообщения
    lines.append("👀 Отфильтрованные адреса:")  # Добавляем заголовок блока
    lines.append("")  # Вставляем пустую строку для визуального разделения

    lines.append(f"• За 180 дней: {cooldown_total}")  # Показываем количество адресов в кулдауне
    lines.append("Примеры 180 дней:")  # Заголовок раздела с примерами кулдауна
    if cooldown_triplet:  # Проверяем наличие примеров для кулдауна
        for email, last_date in cooldown_triplet:  # Перебираем пары адрес-дата
            lines.append(f"• {email} — {last_date}")  # Добавляем строку с адресом и датой
    else:  # Если примеров нет, показываем заглушку
        lines.append("• (нет примеров)")  # Сообщаем пользователю, что примеров нет
    lines.append("")  # Разделяем категории пустой строкой

    suffix_text = f" ({foreign_title_suffix})" if foreign_title_suffix else ""  # Готовим уточнение заголовка
    lines.append(f"• Иностранные домены{suffix_text}: {foreign_total}")  # Отображаем количество иностранных адресов
    lines.append("Примеры иностранных:")  # Заголовок для списка иностранных адресов
    if foreign_triplet:  # Проверяем наличие примеров
        for email in foreign_triplet:  # Перебираем каждую запись
            lines.append(f"• {email}")  # Добавляем адрес в список
    else:  # Если примеров нет, показываем заглушку
        lines.append("• (нет примеров)")  # Сообщаем об отсутствии данных

    return "\n".join(lines)  # Склеиваем строки в итоговый текст
