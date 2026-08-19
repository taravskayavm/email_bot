from __future__ import annotations

import asyncio
from datetime import datetime
import types

import pytest

from emailbot import reporting
from emailbot.reporting import DirectionStats, MOSCOW_TZ, PeriodStats
from emailbot.ui.messages import format_send_stats_by_direction


def _event(ts: str, email: str, group: str, status: str = "success") -> dict[str, str]:
    return {"ts": ts, "email": email, "group": group, "status": status}


def test_month_report_uses_working_hours_aliases_and_deduplicates(monkeypatch):
    events = [
        _event("2026-07-06T07:59:59+03:00", "early@example.com", "highmedicine"),
        _event("2026-07-06T08:00:00+03:00", "one@example.com", "highmedicine"),
        _event("2026-07-06T19:59:59+03:00", "two@example.com", "highmedicine"),
        _event("2026-07-06T20:00:00+03:00", "late@example.com", "highmedicine"),
        _event("2026-07-10T17:29:59+03:00", "friday@example.com", "psychology"),
        _event("2026-07-10T17:30:00+03:00", "friday-late@example.com", "psychology"),
        _event("2026-07-11T10:00:00+03:00", "saturday@example.com", "sport"),
        _event("2026-07-11T17:30:00+03:00", "saturday-late@example.com", "sport"),
        _event("2026-07-07T10:00:00+03:00", "geo@example.com", "география"),
        _event("2026-07-07T10:00:30+03:00", "geo@example.com", "geography"),
        _event("2026-07-07T10:02:00+03:00", "geo@example.com", "geography"),
        _event("2026-07-08T10:00:00+03:00", "error@example.com", "sport", "error"),
        _event("2026-07-08T10:00:00+03:00", "unknown@example.com", "manual"),
    ]
    monkeypatch.setattr(reporting, "_report_source_events", lambda *_args: iter(events))

    stats = reporting.summarize_period_stats(
        "month",
        year=2026,
        month=7,
        now=datetime(2026, 8, 3, 12, 0, tzinfo=MOSCOW_TZ),
    )

    assert stats.date_start.isoformat() == "2026-07-01"
    assert stats.date_end.isoformat() == "2026-07-31"
    assert [(item.code, item.success) for item in stats.directions] == [
        ("geography", 2),
        ("highmedicine", 2),
        ("psychology", 1),
        ("sport", 1),
    ]
    assert stats.total_success == 6
    assert stats.total_failed == 0


def test_week_report_is_current_monday_through_saturday(monkeypatch):
    events = [
        _event("2026-08-03T08:00:00+03:00", "monday@example.com", "sport"),
        _event("2026-08-07T17:29:59+03:00", "friday@example.com", "sport"),
        _event("2026-08-08T17:29:59+03:00", "saturday@example.com", "sport"),
        _event("2026-08-09T10:00:00+03:00", "sunday@example.com", "sport"),
    ]
    monkeypatch.setattr(reporting, "_report_source_events", lambda *_args: iter(events))

    stats = reporting.summarize_period_stats(
        "week", now=datetime(2026, 8, 9, 18, 0, tzinfo=MOSCOW_TZ)
    )

    assert stats.date_start.isoformat() == "2026-08-03"
    assert stats.date_end.isoformat() == "2026-08-08"
    assert stats.total_success == 3


def test_year_report_includes_saturday_values(monkeypatch):
    events = [
        _event("2026-08-08T10:00:00+03:00", "saturday@example.com", "sport"),
        _event("2026-08-09T10:00:00+03:00", "sunday@example.com", "sport"),
    ]
    monkeypatch.setattr(reporting, "_report_source_events", lambda *_args: iter(events))

    stats = reporting.summarize_period_stats(
        "year", year=2026, now=datetime(2026, 8, 10, 12, 0, tzinfo=MOSCOW_TZ)
    )

    assert stats.date_end.isoformat() == "2026-08-10"
    assert stats.total_success == 1


@pytest.mark.parametrize(
    ("period", "year", "month", "expected_start"),
    [
        ("week", None, None, "2026-08-03"),
        ("month", 2026, 8, "2026-08-01"),
        ("year", 2026, None, "2026-01-01"),
    ],
)
def test_current_report_period_ends_on_invocation_date(
    monkeypatch, period, year, month, expected_start
):
    monkeypatch.setattr(reporting, "_report_source_events", lambda *_args: iter(()))

    stats = reporting.summarize_period_stats(
        period,
        year=year,
        month=month,
        now=datetime(2026, 8, 5, 12, 0, tzinfo=MOSCOW_TZ),
    )

    assert stats.date_start.isoformat() == expected_start
    assert stats.date_end.isoformat() == "2026-08-05"


def test_current_period_does_not_count_future_events(monkeypatch):
    events = [
        _event("2026-08-03T09:00:00+03:00", "past@example.com", "sport"),
        _event("2026-08-03T15:00:00+03:00", "future@example.com", "sport"),
    ]
    monkeypatch.setattr(reporting, "_report_source_events", lambda *_args: iter(events))

    stats = reporting.summarize_period_stats(
        "day", now=datetime(2026, 8, 3, 12, 0, tzinfo=MOSCOW_TZ)
    )

    assert stats.total_success == 1


def test_report_message_matches_agreed_layout():
    stats = PeriodStats(
        period="week",
        date_start=datetime(2026, 8, 3).date(),
        date_end=datetime(2026, 8, 7).date(),
        directions=[
            DirectionStats("highmedicine", "Медицина ВО", success=1840),
            DirectionStats("psychology", "Психология", success=206),
        ],
        total_success=2046,
        total_failed=0,
    )

    assert format_send_stats_by_direction(stats) == (
        "📊 Отчёт по отправленным письмам за неделю "
        "03.08.2026–07.08.2026\n\n"
        "Медицина ВО — 1 840\n"
        "Психология — 206\n\n"
        "Всего отправлено: 2 046"
    )


def test_day_report_does_not_show_working_hours():
    stats = PeriodStats(
        period="day",
        date_start=datetime(2026, 8, 3).date(),
        date_end=datetime(2026, 8, 3).date(),
        directions=[DirectionStats("sport", "Физкультура и спорт", success=10)],
        total_success=10,
        total_failed=0,
    )

    message = format_send_stats_by_direction(stats)

    assert message.startswith(
        "📊 Отчёт по отправленным письмам за день 03.08.2026\n\n"
    )
    assert "Рабочее время" not in message


def test_report_selectors_offer_years_and_months(monkeypatch):
    handlers = pytest.importorskip("emailbot.bot_handlers")

    class Message:
        def __init__(self):
            self.texts: list[str] = []
            self.markups: list[object] = []

        async def edit_text(self, text, reply_markup=None, **_kwargs):
            self.texts.append(text)
            self.markups.append(reply_markup)

    class Query:
        def __init__(self, data: str):
            self.data = data
            self.message = Message()

        async def answer(self):
            return None

    monkeypatch.setattr(handlers, "available_report_years", lambda: [2025, 2026])
    query = Query("report_month")
    update = types.SimpleNamespace(callback_query=query)

    asyncio.run(handlers.report_callback(update, types.SimpleNamespace()))

    callbacks = [
        button.callback_data
        for row in query.message.markups[-1].inline_keyboard
        for button in row
    ]
    assert "report_month_year_2025" in callbacks
    assert "report_month_year_2026" in callbacks

    query = Query("report_month_year_2026")
    update = types.SimpleNamespace(callback_query=query)
    asyncio.run(handlers.report_callback(update, types.SimpleNamespace()))
    month_callbacks = [
        button.callback_data
        for row in query.message.markups[-1].inline_keyboard
        for button in row
    ]
    assert "report_month_value_2026_01" in month_callbacks
    assert "report_month_value_2026_12" in month_callbacks
