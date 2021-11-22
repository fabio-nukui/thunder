import asyncio
import logging

log = logging.getLogger(__name__)

MAX_CANCEL_TRIES = 5


async def stop_loop(loop: asyncio.AbstractEventLoop):
    log.info("Stopping event loop", extra={"_sync": True})
    for _ in range(MAX_CANCEL_TRIES):  # loop multiple times to cancel callbacks
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]
        if not tasks:
            break
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    loop.stop()


def raise_task_exception(task: asyncio.Task):
    try:
        if e := task.exception():
            raise e
    except asyncio.CancelledError:
        pass
