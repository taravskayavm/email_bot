from __future__ import annotations

import asyncio
import json
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from emailbot import bot_handlers as handlers, history_service, reporting
from emailbot.reporting import MOSCOW_TZ
from emailbot.ui.messages import format_send_stats_by_direction


def _buttons(markup):
    return [button for row in markup.inline_keyboard for button in row]


@pytest.fixture
def report_ui(monkeypatch):
    edit = AsyncMock()
    summary = Mock(return_value=reporting.PeriodStats(
        period="week", date_start=date(2026, 11, 16), date_end=date(2026, 11, 22),
        directions=[], total_success=0, total_failed=0,
    ))
    years = Mock(return_value=[2025, 2026])
    monkeypatch.setattr(handlers, "_safe_edit_message", edit)
    monkeypatch.setattr(handlers, "summarize_period_stats", summary)
    monkeypatch.setattr(handlers, "available_report_years", years)

    def navigate(data, *, expired=False):
        from telegram.error import BadRequest

        edit.reset_mock()
        query = SimpleNamespace(
            data=data, message=object(),
            answer=AsyncMock(side_effect=BadRequest("Query id is invalid") if expired else None),
        )
        asyncio.run(handlers.report_callback(
            SimpleNamespace(callback_query=query), SimpleNamespace()
        ))
        query.answer.assert_awaited_once()
        edit.assert_awaited_once()
        return edit.call_args.kwargs

    return navigate, summary, years


@pytest.mark.parametrize(
    ("year", "week", "start", "end"),
    [
        (2026, 1, "2025-12-29", "2026-01-04"),
        (2026, 47, "2026-11-16", "2026-11-22"),
        (2026, 48, "2026-11-23", "2026-11-29"),
        (2026, 53, "2026-12-28", "2027-01-03"),
        (2021, 1, "2021-01-04", "2021-01-10"),
    ],
)
def test_selected_week_uses_production_calendar_dates(year, week, start, end):
    assert reporting.report_week_dates(year, week) == (
        date.fromisoformat(start), date.fromisoformat(end)
    )
    bounds = reporting._period_bounds("week", year=year, week=week)
    assert bounds[0].date().isoformat() == start
    assert (bounds[1] - reporting.timedelta(days=1)).date().isoformat() == end
    assert bounds[0].tzinfo == bounds[1].tzinfo == MOSCOW_TZ


def test_week_button_opens_year_selection_instead_of_generating_report(report_ui):
    navigate, summary, years = report_ui

    result = navigate("report_week")

    assert "год, затем неделю" in result["text"]
    assert {button.callback_data for button in _buttons(result["reply_markup"])} == {
        "report_week_year_2025", "report_week_year_2026", "report_menu",
    }
    years.assert_called_once_with(for_week=True)
    summary.assert_not_called()


@pytest.mark.parametrize(("year", "total"), [(2025, 52), (2026, 53)])
def test_week_pages_offer_every_valid_week_once(year, total):
    weeks = []
    for page in range(5):
        buttons = _buttons(handlers._report_week_markup(year, page))
        for button in buttons:
            assert len(button.callback_data.encode("utf-8")) <= 64
            if button.callback_data.startswith("report_week_value_"):
                weeks.append(int(button.callback_data.rsplit("_", 1)[1]))
        callbacks = {button.callback_data for button in buttons}
        assert (f"report_week_page_{year}_{page - 1}" in callbacks) == (page > 0)
        assert (f"report_week_page_{year}_{page + 1}" in callbacks) == (page < 4)
        assert "report_week" in callbacks
    assert weeks == list(range(1, total + 1))


def test_current_year_opens_page_containing_current_week(monkeypatch, report_ui):
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 23, 12, tzinfo=MOSCOW_TZ).astimezone(tz)

    monkeypatch.setattr(handlers, "datetime", FrozenDatetime)
    navigate, summary, _ = report_ui

    result = navigate("report_week_year_2026")
    buttons = _buttons(result["reply_markup"])

    assert any(button.callback_data == "report_week_value_2026_48" for button in buttons)
    assert any(button.text == "№47 · 16.11–22.11" for button in buttons)
    summary.assert_not_called()


def test_week_paging_and_report_back_button_preserve_selected_page(report_ui):
    navigate, summary, _ = report_ui
    page = navigate("report_week_page_2026_3")
    week = next(button for button in _buttons(page["reply_markup"])
                if button.callback_data == "report_week_value_2026_47")

    result = navigate(week.callback_data, expired=True)

    summary.assert_called_once_with("week", year=2026, month=None, week=47)
    assert "неделю №47 (2026)" in result["text"]
    assert "16.11.2026–22.11.2026" in result["text"]
    back = next(button for button in _buttons(result["reply_markup"])
                if button.callback_data == "report_week_page_2026_3")
    result = navigate(back.callback_data)
    assert week.callback_data in {button.callback_data for button in _buttons(result["reply_markup"])}


@pytest.mark.parametrize("data", [
    "report_week_value_2025_53", "report_week_value_2026_00",
    "report_week_value_2026_54", "report_week_value_0000_01",
    "report_week_year_0000", "report_week_page_2026_99",
    "report_week_value_broken", "report_week_page_2026_-1",
])
def test_invalid_week_callbacks_show_recovery_menu_without_querying_history(report_ui, data):
    navigate, summary, _ = report_ui
    result = navigate(data)
    assert "Не удалось" in result["text"]
    assert "report_week" in {button.callback_data for button in _buttons(result["reply_markup"])}
    summary.assert_not_called()


def test_selected_week_reads_durable_history_deduplicates_and_applies_late_bounce(
    monkeypatch, tmp_path
):
    times = [
        "2026-12-27T23:59:59+03:00", "2026-12-28T00:00:00+03:00",
        "2027-01-03T23:59:59+03:00", "2027-01-04T00:00:00+03:00",
    ]
    for index, ts in enumerate(times):
        history_service.mark_sent(
            f"recipient-{index}@example.com", "sport", msg_id=f"message-{index}",
            sent_at=datetime.fromisoformat(ts),
        )
    events = [
        {"ts": times[1], "email": "recipient-1@example.com", "group": "sport",
         "status": "success", "message_id": "message-1"},
        {"ts": "2027-01-05T12:00:00+03:00", "email": "recipient-2@example.com",
         "status": "bounce", "message_id": "message-2", "original_sent_at": times[2]},
    ]
    path = tmp_path / "stats.jsonl"
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")
    monkeypatch.setenv("SEND_STATS_PATH", str(path))

    stats = reporting.summarize_period_stats(
        "week", year=2026, week=53, now=datetime(2027, 1, 10, 12, tzinfo=MOSCOW_TZ)
    )

    assert (stats.date_start, stats.date_end) == (date(2026, 12, 28), date(2027, 1, 3))
    assert (stats.total_success, stats.total_failed) == (1, 1)
    assert "неделю №53 (2026)" in format_send_stats_by_direction(stats)
    assert "28.12.2026–03.01.2027" in format_send_stats_by_direction(stats)


@pytest.mark.parametrize(("week", "count", "start", "end"), [
    (48, 1, "2026-11-23", "2026-11-29"),
    (49, 0, "2026-11-30", "2026-12-06"),
])
def test_current_and_future_selected_weeks_keep_full_dates_without_future_sends(
    monkeypatch, week, count, start, end
):
    events = [
        {"ts": ts, "status": "success", "email": f"recipient-{index}@example.com", "group": "sport"}
        for index, ts in enumerate([
            "2026-11-23T10:00:00+03:00", "2026-11-24T10:00:00+03:00",
            "2026-11-30T10:00:00+03:00",
        ])
    ]
    monkeypatch.setattr(reporting, "_report_source_events", lambda *_: iter(events))

    stats = reporting.summarize_period_stats(
        "week", year=2026, week=week, now=datetime(2026, 11, 23, 12, tzinfo=MOSCOW_TZ)
    )

    assert stats.total_success == count
    assert stats.date_start.isoformat() == start
    assert stats.date_end.isoformat() == end


def test_week_year_selector_uses_iso_year_at_new_year_boundaries(monkeypatch):
    events = [
        {"ts": "2024-12-30T10:00:00+03:00", "status": "success", "group": "sport"},
        {"ts": "2021-01-01T10:00:00+03:00", "status": "success", "group": "sport"},
    ]
    monkeypatch.setattr(reporting, "_report_source_events", lambda *_: iter(events))
    now = datetime(2026, 11, 23, 12, tzinfo=MOSCOW_TZ)

    assert reporting.available_report_years(now, for_week=True) == [2020, 2025, 2026]
    assert reporting.available_report_years(now) == [2021, 2024, 2026]


def test_early_january_offers_the_current_iso_year(monkeypatch):
    monkeypatch.setattr(reporting, "_report_source_events", lambda *_: iter(()))
    assert reporting.available_report_years(
        datetime(2027, 1, 1, 12, tzinfo=MOSCOW_TZ), for_week=True
    ) == [2026, 2027]
