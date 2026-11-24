from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from emailbot import bot_handlers as bh
from emailbot import digest, run_control


@pytest.fixture
def watched_tasks(monkeypatch):
    tasks = []
    targets = []

    async def watchdog(target, **kwargs):
        targets.append(target)
        tasks.append(asyncio.current_task())
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            pass

    monkeypatch.setattr(bh, 'start_watchdog', watchdog)
    return tasks, targets


def update_and_context(*, document=False):
    progress = SimpleNamespace(edit_text=AsyncMock())
    message = SimpleNamespace(
        document=SimpleNamespace(file_name='test.txt') if document else None,
        text='https://example.org/',
        entities=[],
        reply_text=AsyncMock(return_value=progress),
    )
    update = SimpleNamespace(message=message, effective_chat=SimpleNamespace(id=123))
    context = SimpleNamespace(chat_data={}, user_data={}, bot_data={})
    return update, context


@pytest.mark.parametrize('failure', [None, 'error', 'cancelled'])
def test_document_watchdogs_stop_before_shared_update_task_continues(
    monkeypatch,
    tmp_path,
    watched_tasks,
    failure,
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(bh, 'DOWNLOAD_DIR', str(tmp_path))
    monkeypatch.setattr(bh, 'extract_from_uploaded_file', lambda *args: (set(), set(), {}))
    monkeypatch.setattr(bh, 'collect_repairs_from_files', lambda *args: [])
    monkeypatch.setattr(bh, '_compose_report_and_save', AsyncMock(return_value='done'))
    monkeypatch.setattr(bh, '_send_combined_parse_response', AsyncMock())

    async def download(*args):
        await asyncio.sleep(0)
        if failure == 'error':
            raise RuntimeError('download failed')
        if failure == 'cancelled':
            raise asyncio.CancelledError('stopped')
        return str(tmp_path / 'test.txt')

    monkeypatch.setattr(bh, '_download_file', download)

    async def run():
        parent = asyncio.current_task()
        # PTB calls multiple handlers in the same long-lived update task.
        for _ in range(5):
            update, context = update_and_context(document=True)
            await bh.handle_document(update, context)
            assert all(task.done() for task in watched_tasks[0])
            assert run_control.get_task(bh._build_parse_task_name(update, 'file')) is None
        assert watched_tasks[1] == [parent] * 5
        assert parent.cancelling() == 0

    asyncio.run(run())


@pytest.mark.parametrize('failure', [None, 'read_timeout', 'cancelled', 'status_cancelled'])
def test_url_watchdog_stops_on_success_error_and_cancellation(monkeypatch, watched_tasks, failure):
    monkeypatch.setattr(bh.settings, 'ENABLE_WEB', True)

    async def extract(*args, **kwargs):
        await asyncio.sleep(0)
        if failure == 'read_timeout':
            raise httpx.ReadTimeout('temporary')
        if failure == 'cancelled':
            raise asyncio.CancelledError('stopped')
        return []

    monkeypatch.setattr(digest, 'extract_from_url', extract)

    async def run():
        update, context = update_and_context()
        if failure == 'status_cancelled':

            async def cancelled_status(*args, **kwargs):
                await asyncio.sleep(0)
                raise asyncio.CancelledError('stopped')

            update.message.reply_text = cancelled_status
        if failure in ('cancelled', 'status_cancelled'):
            with pytest.raises(asyncio.CancelledError):
                await bh.handle_url_text(update, context)
        else:
            await bh.handle_url_text(update, context)
        assert len(watched_tasks[0]) == 1
        assert watched_tasks[0][0].done()
        assert run_control.get_task(bh._build_parse_task_name(update, 'url')) is None
        assert asyncio.current_task().cancelling() == 0

    asyncio.run(run())
