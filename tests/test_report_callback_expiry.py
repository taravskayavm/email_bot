import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from telegram.error import BadRequest

from emailbot import bot_handlers as handlers


@pytest.mark.parametrize("error", [
    None,
    "Query is too old and response timeout expired or query id is invalid",
    "Query id is invalid",
])
@pytest.mark.parametrize("data", ["report_menu", "report_day"])
def test_report_continues_after_expired_ack(monkeypatch, error, data):
    query = SimpleNamespace(
        answer=AsyncMock(side_effect=BadRequest(error) if error else None),
        data=data,
        message=object(),
    )
    edit = AsyncMock()
    summarize = Mock(return_value=object())
    monkeypatch.setattr(handlers, "_safe_edit_message", edit)
    monkeypatch.setattr(handlers, "summarize_period_stats", summarize)
    monkeypatch.setattr(handlers, "format_send_stats_by_direction", lambda _: "Report")

    asyncio.run(handlers.report_callback(SimpleNamespace(callback_query=query), SimpleNamespace()))

    query.answer.assert_awaited_once_with()
    edit.assert_awaited_once()
    assert edit.call_args.args[0] is query.message
    if data == "report_day":
        summarize.assert_called_once_with("day", year=None, month=None)
        assert edit.call_args.kwargs["text"] == "Report"
    else:
        summarize.assert_not_called()


def test_report_does_not_hide_unrelated_bad_request(monkeypatch):
    error = BadRequest("Chat not found")
    query = SimpleNamespace(answer=AsyncMock(side_effect=error), data="report_menu")
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_safe_edit_message", edit)

    with pytest.raises(BadRequest) as caught:
        asyncio.run(handlers.report_callback(SimpleNamespace(callback_query=query), SimpleNamespace()))

    assert caught.value is error
    edit.assert_not_awaited()

