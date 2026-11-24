"""Sequential off-loop operations with cooperative cancellation between messages."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from functools import partial
from functools import wraps


def with_mail_worker(function):
    @wraps(function)
    async def wrapped(*args, **kwargs):
        worker = MailWorker()
        try:
            return await function(*args, **kwargs, _mail_worker=worker)
        finally:
            worker.close()
    return wrapped


class MailWorker:
    """Own connections in one thread and drain in-flight I/O before closing them.

    A cancelled await cannot interrupt smtplib safely. Finish that operation and
    return its result for bookkeeping; the batch then checks ``cancel_requested``.
    """

    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="email-transport")
        self.cancel_requested = False

    async def run(self, function, *args, **kwargs):
        future = asyncio.get_running_loop().run_in_executor(
            self.executor, partial(function, *args, **kwargs)
        )
        while True:
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                self.cancel_requested = True

    @asynccontextmanager
    async def session(self, context_manager):
        value = await self.run(context_manager.__enter__)
        try:
            yield value
        except BaseException as exc:
            if not await self.run(context_manager.__exit__, type(exc), exc, exc.__traceback__):
                raise
        else:
            await self.run(context_manager.__exit__, None, None, None)

    def close(self):
        # run() always drains pending work, so no live socket operation remains.
        self.executor.shutdown(wait=True)
