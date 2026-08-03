import asyncio

import pytest

from emailbot.run_control import (
    clear_stop,
    register_task,
    should_stop,
    stop_and_status,
)


@pytest.mark.asyncio
async def test_stop_cancels_every_registered_task():
    clear_stop()
    started = asyncio.Event()

    async def worker():
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(worker())
    register_task("test-worker", task)
    await started.wait()

    status = stop_and_status()
    result = await asyncio.gather(task, return_exceptions=True)

    assert "test-worker" in status["running"]
    assert isinstance(result[0], asyncio.CancelledError)
    assert should_stop() is True
    clear_stop()
