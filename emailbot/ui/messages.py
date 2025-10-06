from __future__ import annotations
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
    lines.append("Дополнительные действия:")
    lines.append("  ⬜ Показать ещё примеры")
    lines.append("  🧭 Перейти к выбору направления")
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


def format_dispatch_result(total: int, sent: int, cooldown_skipped: int, blocked: int) -> str:
    left = max(total - sent - cooldown_skipped - blocked, 0)
    return (
        "📨 Рассылка завершена.\n"
        f"📊 В очереди было: {total}\n"
        f"✅ Отправлено: {sent}\n"
        f"⏳ Пропущены (180-дневная неплотность): {cooldown_skipped}\n"
        f"🚫 В блок-листе/недоступны: {blocked}\n"
        f"ℹ️ Осталось без изменений: {left}"
    )
