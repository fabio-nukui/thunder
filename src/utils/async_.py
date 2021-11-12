import asyncio
import logging

log = logging.getLogger(__name__)


async def stop_loop(loop: asyncio.AbstractEventLoop):
    log.info(f"Stopping {loop=}")
    tasks = [task for task in asyncio.all_tasks(loop) if task is not asyncio.current_task(loop)]
    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()
