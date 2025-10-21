from __future__ import annotations

from typing import Iterable, Mapping

# ВАЖНО: не делаем жёсткий импорт на уровне модуля — возможны циклические импорты
_HAVE_COUNT_BLOCKED = True
try:
    from emailbot.reporting import count_blocked  # type: ignore
except Exception:  # pragma: no cover - используем отложенный импорт
    count_blocked = None  # type: ignore[assignment]
    _HAVE_COUNT_BLOCKED = False

# Старый «приятный» стиль сообщений под Telegram (эмодзи + плотные подпункты).
# Никакого HTML – чистый текст/Markdown-safe (aiogram parse_mode="HTML"/"MarkdownV2" на твой выбор).


def format_parse_summary(s: Mapping[str, object], examples: Iterable[str] = ()) -> str:
    """
    Ожидаемые ключи s:
      total_found, to_send, suspicious, cooldown_180d, foreign_domain,
      pages_skipped, footnote_dupes_removed, blocked, blocked_after_parse
    """
    lines = []
    lines.append("✅ Анализ завершён.")
    lines.append(f"Найдено адресов: {s.get('total_found', 0)}")
    lines.append(f"📦 К отправке: {s.get('to_send', 0)}")
    lines.append(f"🟡 Подозрительные: {s.get('suspicious', 0)}")
    lines.append(f"⏳ Под кулдауном (180 дней): {s.get('cooldown_180d', 0)}")
    lines.append(f"🌍 Иностранные домены: {s.get('foreign_domain', 0)}")
    lines.append(f"📄 Пропущено страниц: {s.get('pages_skipped', 0)}")
    lines.append(f"♻️ Возможные сносочные дубликаты удалены: {s.get('footnote_dupes_removed', 0)}")
    try:
        ocr_total = int(s.get("ocr_fix_total", 0) or 0)
        ocr_space = int(s.get("ocr_fix_space_tld", 0) or 0)
        ocr_comma = int(s.get("ocr_fix_comma_tld", 0) or 0)
    except Exception:
        ocr_total = ocr_space = ocr_comma = 0
    if ocr_total > 0:
        lines.append(
            "🧹 Исправления OCR: "
            f"{ocr_total} (восстановлена точка перед зоной "
            f"(пробел/символ/перенос): {ocr_space}; "
            f"запятая→точка: {ocr_comma})"
        )
    try:
        blocked_before = int(s.get('blocked', 0) or 0)
    except Exception:
        blocked_before = 0
    try:
        blocked_after = int(s.get('blocked_after_parse', 0) or 0)
    except Exception:
        blocked_after = 0
    total_blocked = blocked_before + blocked_after
    if total_blocked > 0:
        lines.append(f"🚫 В стоп-листе: {total_blocked}")
    lines.append("")

    def _append_examples(title: str, key: str) -> bool:
        values = s.get(key)
        if not values:
            return False
        if isinstance(values, str):
            iterable = [values]
        else:
            try:
                iterable = list(values)
            except TypeError:
                iterable = [values]
        samples = [str(item).strip()[:80] for item in iterable if str(item).strip()]
        if not samples:
            return False
        lines.append(title)
        for sample in samples:
            lines.append(f" • {sample}")
        return True

    appended = False
    appended |= _append_examples("❗ Примеры некорректных доменов:", "invalid_tld_examples")
    appended |= _append_examples("🚫 Синтаксические отказы:", "syntax_fail_examples")
    appended |= _append_examples("🔁 Исправлены гомоглифы:", "confusable_fixed_examples")
    if appended:
        lines.append("")

    return "\n".join(lines)


def format_direction_selected(name_ru: str, code: str | None = None) -> str:
    if code:
        return f"✅ Выбран шаблон: «{name_ru}» ({code})"
    return f"✅ Выбран шаблон: «{name_ru}»"


def format_dispatch_preview(stats: Mapping[str, int], xlsx_name: str) -> str:
    """
    Ожидаемые ключи:
      ready_to_send, deferred_180d, in_blacklists, need_review
    """
    return (
        f"📎 {xlsx_name}\n"
        f"🚀 Готово к отправке: {stats.get('ready_to_send', 0)} адресов.\n"
        f"⏳ Отложено по правилу 180 дн.: {stats.get('deferred_180d', 0)}\n"
        f"🧱 В исключениях/стоп-листах: {stats.get('in_blacklists', 0)}\n"
        f"🔍 Требует проверки: {stats.get('need_review', 0)}\n"
        f"Файл-предпросмотра: подробности внутри."
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
    lines = [
        "✉️ Рассылка начата.",
        f"Запрошено: {planned}",
        f"Уникальных: {unique}",
    ]
    if limited_from is not None and limited_from > to_send:
        lines.append(
            f"К отправке (после фильтров и лимитов): {to_send} из {limited_from}"
        )
    else:
        lines.append(f"К отправке (после фильтров): {to_send}")
    if deferred:
        lines.append(f"Отложено по правилу 180 дней: {deferred}")
    if suppressed:
        lines.append(f"Исключено (супресс/стоп-лист): {suppressed}")
    if foreign:
        lines.append(f"Отложено (иностранные домены): {foreign}")
    if duplicates:
        lines.append(f"Дубликаты в пачке: {duplicates}")
    return "\n".join(lines)


def format_dispatch_result(
    total: int,
    sent: int,
    cooldown_skipped: int,
    blocked: int,
    duplicates: int = 0,
    *,
    aborted: bool = False,
) -> str:
    left = max(total - sent - cooldown_skipped - blocked - duplicates, 0)
    lines = [
        "📨 Рассылка завершена.",
        f"📊 В очереди было: {total}",
        f"✅ Отправлено: {sent}",
        f"⏳ Пропущены (по правилу «180 дней»): {cooldown_skipped}",
        f"🚫 В стоп-листе: {blocked}",
    ]
    if duplicates:
        lines.append(f"🔁 Дубликаты за 24 ч: {duplicates}")
    lines.append(f"ℹ️ Осталось без изменений: {left}")
    if aborted:
        lines.append("🛑 Процесс был остановлен по запросу.")
    return "\n".join(lines)


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
    total_skipped = max(skipped_cooldown, skipped_initial)
    planned_materialized: list[str] | None = None
    planned_display = planned
    if planned_emails is not None:
        planned_materialized = list(planned_emails)
        planned_display = len(planned_materialized)
    final_blocked = blocked_count
    if final_blocked is None:
        blocked_source = (
            planned_materialized
            if planned_materialized is not None
            else planned_emails
        ) or raw_emails or []
        final_blocked = 0
        try:
            global count_blocked, _HAVE_COUNT_BLOCKED
            if not _HAVE_COUNT_BLOCKED:
                from emailbot.reporting import count_blocked as _count_blocked  # type: ignore

                count_blocked = _count_blocked  # type: ignore[assignment]
                _HAVE_COUNT_BLOCKED = True
            if callable(count_blocked):
                final_blocked = count_blocked(blocked_source)  # type: ignore[arg-type]
        except Exception:
            final_blocked = 0

    audit_suffix = f"\n\n📄 Аудит: {audit_path}" if audit_path else ""
    return (
        "📨 Рассылка завершена.\n"
        f"📊 В очереди было: {planned_display}\n"
        f"✅ Отправлено: {sent}\n"
        f"⏳ Пропущены (по правилу «180 дней»): {total_skipped}\n"
        f"🚫 В стоп-листе: {final_blocked}\n"
        "ℹ️ Осталось без изменений: 0\n"
        f"❌ Ошибок при отправке: {errors}"
        f"{audit_suffix}"
    )


def format_error_details(details: Iterable[str]) -> str:
    """Return an empty string to avoid sending hidden error summaries."""

    return ""
