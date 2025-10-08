from __future__ import annotations
import re
from collections import Counter
from typing import Iterable, Mapping

# Старый «приятный» стиль сообщений под Telegram (эмодзи + плотные подпункты).
# Никакого HTML – чистый текст/Markdown-safe (aiogram parse_mode="HTML"/"MarkdownV2" на твой выбор).

def format_parse_summary(s: Mapping[str, int], examples: Iterable[str] = ()) -> str:
    """
    Ожидаемые ключи s:
      total_found, to_send, suspicious, cooldown_180d, foreign_domain,
      pages_skipped, footnote_dupes_removed
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
    lines.append("")
    ex = list(examples)
    if ex:
        lines.append("📝 Примеры:")
        for e in ex[:10]:
            lines.append(f"• {e}")
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
        f"🧱 В исключениях/блок-листах: {stats.get('in_blacklists', 0)}\n"
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
        lines.append(f"Исключено (супресс/блок-лист): {suppressed}")
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
) -> str:
    left = max(total - sent - cooldown_skipped - blocked - duplicates, 0)
    lines = [
        "📨 Рассылка завершена.",
        f"📊 В очереди было: {total}",
        f"✅ Отправлено: {sent}",
        f"⏳ Пропущены (по правилу «180 дней»): {cooldown_skipped}",
        f"🚫 В блок-листе/недоступны: {blocked}",
    ]
    if duplicates:
        lines.append(f"🔁 Дубликаты за 24 ч: {duplicates}")
    lines.append(f"ℹ️ Осталось без изменений: {left}")
    return "\n".join(lines)


_EMAIL_RE = re.compile(r"(?i)[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}")


def format_error_details(details: Iterable[str]) -> str:
    """Format a summary of error reasons without exposing e-mail addresses."""

    sanitized: list[str] = []
    for item in details:
        text = str(item).strip()
        if not text:
            continue
        sanitized.append(_EMAIL_RE.sub("[скрыто]", text))

    if not sanitized:
        return ""

    counts = Counter(sanitized)
    lines = ["Ошибки (адреса скрыты):"]
    for reason, count in counts.most_common():
        if not reason:
            continue
        if count > 1:
            lines.append(f"• {reason} ×{count}")
        else:
            lines.append(f"• {reason}")

    if len(lines) == 1:
        lines.append(f"• Всего ошибок: {len(sanitized)}")

    return "\n".join(lines)
