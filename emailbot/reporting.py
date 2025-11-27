"""Utilities for composing user-facing reports."""

from __future__ import annotations

# Импортируем json для сериализации и десериализации событий отправки.
import json
# Подключаем logging для записи диагностических сообщений.
import logging
# Используем os для работы с путями и переменными окружения.
import os
# Поддерживаем как текущие отметки времени, так и вычисления периодов отчётности.
from dataclasses import dataclass
# Работаем с датами и временными диапазонами отчётов.
from datetime import date, datetime, timedelta, timezone
# Оперируем путями к файлам статистики и конфигурации.
from pathlib import Path
# Используем расширенные типы аннотаций для повышения читаемости кода.
from typing import Dict, Iterable, List, Mapping, Optional, TYPE_CHECKING, Tuple

# Задаём тайм-зону Москвы для корректного сопоставления событий.
from zoneinfo import ZoneInfo

# Импортируем утилиту для преобразования кодов направлений в названия.
from emailbot.directions import resolve_direction_title
# Используем проверку блок-листа для подсчёта заблокированных адресов.
from emailbot.suppress_list import is_blocked
# Пишем статистику отправок в JSONL-файл атомарно.
from emailbot.utils.fs import append_jsonl_atomic

if TYPE_CHECKING:  # pragma: no cover - typing hints only
    from emailbot.report_preview import PreviewData


def _now_ts() -> str:
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"


_DIGEST_LOGGER = logging.getLogger("emailbot.digest")
_DEBUG_INVALID_TLD_SUMMARY = os.getenv("DEBUG_INVALID_TLD_SUMMARY", "0") == "1"


def log_extract_digest(stats: dict) -> None:
    """Log a one-line JSON digest for extraction statistics."""

    data = {
        "ts": _now_ts(),
        "level": "INFO",
        "component": "extract",
        "footnote_singletons_repaired": stats.get("footnote_singletons_repaired", 0),
        "footnote_guard_skips": stats.get("footnote_guard_skips", 0),
        "footnote_ambiguous_kept": stats.get("footnote_ambiguous_kept", 0),
        "left_guard_skips": stats.get("left_guard_skips", 0),
        "prefix_expanded": stats.get("prefix_expanded", 0),
        "phone_prefix_stripped": stats.get("phone_prefix_stripped", 0),
    }
    data.update(stats)
    _DIGEST_LOGGER.info(json.dumps(data, ensure_ascii=False))


def log_mass_filter_digest(ctx: dict) -> None:
    """Log a one-line JSON digest for mass-mail filter statistics."""

    data = {"ts": _now_ts(), "level": "INFO", "component": "mass_filter"}
    data.update(ctx)
    _DIGEST_LOGGER.info(json.dumps(data, ensure_ascii=False))


def render_summary(stats: dict) -> str:
    """Render a short textual summary for extraction statistics."""

    lines: List[str] = []

    total_found = stats.get("total_found")
    if total_found is not None:
        lines.append(f"📊 Найдено адресов: {total_found}")

    to_send = stats.get("unique_after_cleanup")
    if to_send is None:
        to_send = stats.get("total_ready", 0)
    lines.append(f"📦 К отправке: {to_send}")

    suspicious = stats.get("suspicious_numeric_localpart")
    if suspicious:
        lines.append(f"🟡 Подозрительные: {suspicious}")

    blocked_total = stats.get("blocked_total", 0)
    lines.append(f"🚫 Из стоп-листа: {blocked_total}")

    missed_pages = stats.get("pdf_pages_failed")
    if missed_pages:
        lines.append(f"📄 Не распознаны страницы PDF: {missed_pages}")

    needs_ocr = stats.get("needs_ocr")
    if needs_ocr:
        lines.append("💡 Включите OCR для лучшего извлечения")

    invalid_tld = stats.get("invalid_tld")
    if invalid_tld:
        line = f"❗ Некорректные домены: {invalid_tld}"
        if _DEBUG_INVALID_TLD_SUMMARY:
            examples = stats.get("invalid_tld_examples") or []
            if examples:
                sample = ", ".join(list(dict.fromkeys(examples))[:3])
                line = f"{line} ({sample})"
        lines.append(line)

    return "\n".join(lines)


def build_mass_report_text(
    sent_ok: Iterable[str],
    skipped_recent: Iterable[str],
    blocked_foreign: Optional[Iterable[str]] = None,
    blocked_invalid: Optional[Iterable[str]] = None,
    duplicates_24h: Optional[Iterable[str]] = None,
) -> str:
    """Build summary text for mass mailing.

    The function returns only aggregate counts without revealing individual
    e‑mail addresses. ``blocked_foreign`` and ``blocked_invalid`` are accepted for
    backward compatibility and counted in the summary.
    """

    sent_cnt = len(list(sent_ok))
    skipped_cnt = len(list(skipped_recent))
    blocked_cnt = len(list(blocked_invalid or []))
    foreign_cnt = len(list(blocked_foreign or []))
    dup_cnt = len(list(duplicates_24h or []))
    total = sent_cnt + skipped_cnt + blocked_cnt + foreign_cnt + dup_cnt

    lines = [
        "✉️ Рассылка завершена.",
        f"📦 В очереди было: {total}",
        f"✅ Успешно отправлено: {sent_cnt}",
        f"⏳ Пропущены (по правилу «180 дней»): {skipped_cnt}",
        f"🚫 В стоп-листе: {blocked_cnt}",
        f"🌍 Иностранные (отложены): {foreign_cnt}",
    ]
    if dup_cnt:
        lines.append(f"🔁 Дубликаты за 24 ч: {dup_cnt}")
    return "\n".join(lines)


def count_blocked(emails: Iterable[str]) -> int:
    """Возвращает, сколько адресов присутствует в блок-листе.

    Никогда не бросает исключения — при любой ошибке возвращает 0.
    """

    if not emails:
        return 0

    try:
        return sum(1 for email in emails if email and is_blocked(email))
    except Exception:
        logging.getLogger(__name__).debug("count_blocked failed", exc_info=True)
        return 0


def _stats_path(path_override: str | None = None) -> Path:
    raw = path_override or os.getenv("SEND_STATS_PATH", "var/send_stats.jsonl")
    expanded = os.path.expanduser(os.path.expandvars(str(raw)))
    path = Path(expanded)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _preview_group_value(data: "PreviewData") -> str:
    group_code = getattr(data, "group_code", "") or ""
    group_label = getattr(data, "group", "") or ""
    candidate = group_code.strip() or group_label.strip()
    return candidate


def _collect_preview_rows(data: "PreviewData") -> tuple[str, str, list[dict[str, str]]]:
    run_id = (getattr(data, "run_id", "") or "").strip()
    if not run_id:
        return "", "", []
    group_value = _preview_group_value(data)
    sections = [
        list(getattr(data, "valid", []) or []),
        list(getattr(data, "rejected_180d", []) or []),
        list(getattr(data, "blocked", []) or []),
        list(getattr(data, "foreign", []) or []),
        list(getattr(data, "suspicious", []) or []),
        list(getattr(data, "duplicates", []) or []),
    ]
    rows: list[dict[str, str]] = []
    for section in sections:
        for row in section:
            if not isinstance(row, dict):
                continue
            email = str(row.get("email") or "").strip()
            if not email:
                continue
            reason = str(row.get("reason") or "").strip()
            if not reason:
                continue
            source_value = row.get("source") or row.get("source_files") or ""
            source = str(source_value).strip()
            rows.append({"email": email, "reason": reason, "source": source})
    return run_id, group_value, rows


def write_preview_stats(data: "PreviewData", *, stats_path: str | None = None) -> None:
    """Append preview classification rows to ``SEND_STATS_PATH``."""

    if data is None:
        return
    run_id, group_value, rows = _collect_preview_rows(data)
    if not run_id or not rows:
        return
    path = _stats_path(stats_path)
    for row in rows:
        payload = {
            "ts": _now_ts(),
            "email": row["email"],
            "reason": row["reason"],
            "source": row["source"],
            "group": group_value,
            "run_id": run_id,
        }
        append_jsonl_atomic(path, payload)


# === Статистика отправок по периодам и направлениям ===


# Фиксируем тайм-зону Москвы для приведения отметок времени событий.
MOSCOW_TZ = ZoneInfo("Europe/Moscow")


# Описываем счётчики по отдельному направлению отправок.
@dataclass
class DirectionStats:
    # Сохраняем код направления для дальнейшей идентификации.
    code: str
    # Запоминаем человеко-читаемое название направления.
    title: str
    # Счётчик успешных отправок по направлению.
    success: int = 0
    # Счётчик ошибочных или неудачных отправок.
    failed: int = 0


# Агрегируем показатели за период сразу по всем направлениям.
@dataclass
class PeriodStats:
    # Сохраняем идентификатор периода (day/week/month/year).
    period: str
    # Отмечаем начальную дату периода в московском времени.
    date_start: date
    # Фиксируем конечную дату периода в московском времени.
    date_end: date
    # Перечисляем детализированные показатели по направлениям.
    directions: List[DirectionStats]
    # Число успешных отправок суммарно за период.
    total_success: int
    # Число неуспешных отправок суммарно за период.
    total_failed: int


def _parse_event_timestamp(event: Mapping[str, object]) -> datetime | None:
    """
    Попробовать извлечь метку времени события в UTC и привести к aware datetime.

    Поддерживаем несколько возможных ключей для совместимости:
      "timestamp", "ts", "time"
    """

    # Пытаемся найти значение временной отметки в поддерживаемых полях.
    raw = event.get("timestamp") or event.get("ts") or event.get("time")
    # Если ничего не найдено, возвращаем None.
    if not raw:
        return None
    # Приводим значение к строке и удаляем пробелы.
    text = str(raw).strip()
    # Проверяем, что после очистки строка не пуста.
    if not text:
        return None
    # Рассматриваем вариант строки с символом 'Z' для обозначения UTC.
    if text.endswith("Z"):
        # Удаляем символ 'Z', потому что fromisoformat не поддерживает его напрямую.
        text = text[:-1]
        # Пытаемся разобрать строку с учётом удалённого индикатора UTC.
        try:
            # Пробуем разобрать строку как ISO 8601.
            dt_obj = datetime.fromisoformat(text)
            # Если временная зона не указана, явно присваиваем UTC.
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
        except Exception:
            # При любой ошибке парсинга возвращаем None.
            return None
    else:
        # Аналогично выполняем попытку парсинга строки без символа 'Z'.
        try:
            # Аналогично разбираем строку без символа 'Z'.
            dt_obj = datetime.fromisoformat(text)
            # Если временная зона отсутствует, считаем отметку UTC.
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
        except Exception:
            # При ошибке возвращаем None для пропуска события.
            return None
    # Возвращаем нормализованное значение даты и времени.
    return dt_obj


def _event_direction_code(event: Mapping[str, object]) -> str | None:
    """
    Извлечь код направления из события.

    Каноническое поле — "group". Если его нет или оно пустое, направление не учитываем.
    """

    # Получаем значение поля group, если оно присутствует.
    value = event.get("group")
    # Если значение пустое, прекращаем обработку.
    if not value:
        return None
    # Приводим код направления к строке и удаляем пробелы.
    code = str(value).strip()
    # Возвращаем код, если он не пустой, иначе None.
    return code or None


def _event_success_failed(event: Mapping[str, object]) -> Tuple[int, int]:
    """
    Определить, считается ли событие успешным или ошибочным.

    Ожидается поле result или status с текстовыми значениями:
      sent / success / ok     -> успех
      failed / error / bounce -> ошибка
    Всё остальное игнорируется.
    """

    # Извлекаем поле со статусом обработки отправки.
    raw = event.get("result") or event.get("status")
    # Если поле отсутствует, возвращаем нулевые счётчики.
    if not raw:
        return 0, 0
    # Приводим значение к нижнему регистру для сопоставления.
    value = str(raw).strip().lower()
    # Для позитивных значений считаем событие успешным.
    if value in {"sent", "success", "ok"}:
        return 1, 0
    # Для негативных значений считаем событие ошибочным.
    if value in {"failed", "error", "bounce"}:
        return 0, 1
    # Во всех остальных случаях событие не учитываем.
    return 0, 0


def _period_bounds(period: str, now: datetime | None = None) -> Tuple[datetime, datetime]:
    """
    Рассчитать границы периода [start, end) в часовом поясе Москвы.

    period:
      "day"   – текущий день;
      "week"  – последние 7 дней (включая сегодня);
      "month" – последние 30 дней (включая сегодня);
      "year"  – последние 365 дней (включая сегодня).
    """

    # Если текущая отметка времени не передана, берём актуальное время в Москве.
    if now is None:
        now_msk = datetime.now(tz=MOSCOW_TZ)
    else:
        # Если временная зона отсутствует, принудительно задаём московскую.
        if now.tzinfo is None:
            now_msk = now.replace(tzinfo=MOSCOW_TZ)
        else:
            # В противном случае конвертируем время в московскую зону.
            now_msk = now.astimezone(MOSCOW_TZ)

    # Выделяем только дату для дальнейших вычислений диапазона.
    today = now_msk.date()

    # Подбираем границы периода в зависимости от значения аргумента.
    if period == "day":
        start_date = today
        end_date = today + timedelta(days=1)
    elif period == "week":
        start_date = today - timedelta(days=6)
        end_date = today + timedelta(days=1)
    elif period == "month":
        start_date = today - timedelta(days=29)
        end_date = today + timedelta(days=1)
    elif period == "year":
        start_date = today - timedelta(days=364)
        end_date = today + timedelta(days=1)
    else:
        # Сообщаем об ошибке при неизвестном периоде.
        raise ValueError(f"Unknown period: {period!r}")

    # Собираем начало периода как datetime в московской зоне.
    start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=MOSCOW_TZ)
    # Аналогично рассчитываем верхнюю границу периода.
    end_dt = datetime.combine(end_date, datetime.min.time()).replace(tzinfo=MOSCOW_TZ)

    # Возвращаем полуинтервал [start_dt, end_dt).
    return start_dt, end_dt


def _default_send_stats_path() -> Path:
    """
    Путь к send_stats.jsonl относительно корня проекта.

    При необходимости можно заменить на конфиг из config.py.
    """

    # Определяем корень проекта по положению текущего файла.
    project_root = Path(__file__).resolve().parent.parent
    # Формируем полный путь к файлу статистики отправок.
    return project_root / "send_stats.jsonl"


def _iter_send_events(path: str | Path | None = None) -> Iterable[Mapping[str, object]]:
    """
    Пройти по событиям отправки из send_stats.jsonl.

    Формат: JSON Lines, по одному объекту на строку.
    """

    # Приводим аргумент пути к объекту Path, используя значение по умолчанию при необходимости.
    stats_path = Path(path) if path is not None else _default_send_stats_path()
    # Проверяем наличие файла и сразу возвращаем пустой генератор при его отсутствии.
    if not stats_path.exists():
        # Возвращаем пустой список, если файл не найден.
        return []

    # Определяем вложенную функцию-генератор для ленивой обработки строк файла.
    def _gen() -> Iterable[Mapping[str, object]]:
        # Открываем файл в кодировке UTF-8 с поддержкой BOM, чтобы корректно
        # обрабатывать как файлы без BOM, так и с BOM (utf-8-sig).
        with stats_path.open("r", encoding="utf-8-sig") as file_obj:
            for line in file_obj:
                # Удаляем перевод строки и пропускаем пустые строки.
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    # Преобразуем строку JSON в объект Python.
                    obj = json.loads(stripped)
                except Exception:
                    # При ошибке парсинга пропускаем конкретную строку.
                    continue
                # Убеждаемся, что получили словарь, прежде чем отдавать результат.
                if isinstance(obj, dict):
                    yield obj

    # Возвращаем генератор для последующей итерации вне функции.
    return _gen()


def summarize_period_stats(period: str) -> PeriodStats:
    """
    Подсчитать статистику отправок за заданный период по направлениям.

    Периоды: "day", "week", "month", "year".
    """

    # Вычисляем границы выбранного периода в московской временной зоне.
    start_dt, end_dt = _period_bounds(period)
    # Переводим границы в московскую зону (на случай будущего изменения логики).
    start_msk = start_dt.astimezone(MOSCOW_TZ)
    end_msk = end_dt.astimezone(MOSCOW_TZ)

    # Инициализируем словарь накопителей по направлениям.
    per_dir: Dict[str, DirectionStats] = {}
    # Готовим агрегированные счётчики успешных отправок.
    total_success = 0
    # И аналогичные счётчики для неудачных попыток.
    total_failed = 0

    # Перебираем события отправок из журнала.
    for event in _iter_send_events():
        # Парсим отметку времени события.
        ts = _parse_event_timestamp(event)
        # Пропускаем события без корректной временной отметки.
        if ts is None:
            continue
        # Переводим отметку в московское время для сравнения с границами периода.
        ts_msk = ts.astimezone(MOSCOW_TZ)
        # Пропускаем события, выходящие за пределы периода.
        if not (start_msk <= ts_msk < end_msk):
            continue

        # Извлекаем код направления рассылки.
        code = _event_direction_code(event)
        # Не учитываем события без направления.
        if not code:
            continue

        # Получаем название направления по коду.
        title = resolve_direction_title(code)
        # Игнорируем направления, не известные в справочнике.
        if not title:
            continue

        # Определяем, успешна ли отправка.
        success, failed = _event_success_failed(event)
        # Пропускаем события без явного исхода.
        if success == 0 and failed == 0:
            continue

        # Создаём запись для направления, если она ещё не инициализирована.
        if code not in per_dir:
            per_dir[code] = DirectionStats(code=code, title=title, success=0, failed=0)  # Инициализируем пустые счётчики по направлению.

        # Получаем накопитель по направлению.
        stats_obj = per_dir[code]
        # Увеличиваем счётчики успешных отправок.
        stats_obj.success += success
        # Увеличиваем счётчики ошибок отправки.
        stats_obj.failed += failed

        # Дополняем суммарные показатели за период.
        total_success += success
        total_failed += failed

    # Сортируем направления по количеству успешных отправок и названию.
    directions_sorted = sorted(
        # Получаем коллекцию направлений для сортировки.
        per_dir.values(),
        # Сортируем сперва по убыванию успешных отправок, затем по названию.
        key=lambda direction: (-direction.success, direction.title.lower()),
    )

    # Возвращаем итоговую структуру PeriodStats с агрегированными данными.
    return PeriodStats(
        # Указываем идентификатор периода для итогового отчёта.
        period=period,
        # Сохраняем начальную дату периода.
        date_start=start_msk.date(),
        # Фиксируем конечную дату периода (включительно).
        date_end=(end_msk - timedelta(days=1)).date(),
        # Прикладываем отсортированные показатели по направлениям.
        directions=directions_sorted,
        # Возвращаем общее число успешных отправок.
        total_success=total_success,
        # Возвращаем общее число неудачных отправок.
        total_failed=total_failed,
    )
