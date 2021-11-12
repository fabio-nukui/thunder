import asyncio
import importlib
import logging
import signal
from functools import partial

import configs
import utils
from exceptions import NodeSyncing
from startup import setup

log = logging.getLogger(__name__)

GIT_COMMIT = open("git_commit").read().strip()


async def run_strategy(strategy_name: str):
    log.info(f"Running on git commit {GIT_COMMIT}")
    strategy = importlib.import_module(f"strategies.{strategy_name}")
    while True:
        try:
            log.info(f"Starting strategy {strategy_name}")
            await strategy.run()  # type: ignore
        except NodeSyncing as e:
            log.info(f"Node syncing to blockchain, latest height={e.latest_height}")
            log.info("Restarting strategy in 60 seconds")
            await asyncio.sleep(60)
        except Exception:
            log.error("Error during strategy execution", exc_info=True)
            log.info("Restarting strategy in 5 seconds")
            await asyncio.sleep(5)
        utils.cache.clear_caches(utils.cache.CacheGroup.ALL, clear_all=True)


async def shutdown(loop: asyncio.AbstractEventLoop, signal: signal.Signals = None):
    if signal:
        log.info(f"Received exit signal {signal.name}")
    log.info("Shutting down")
    await utils.async_.stop_loop(loop)


def handle_exception(loop: asyncio.AbstractEventLoop, context: dict):
    msg = context.get("exception", context["message"])
    log.error(f"Unexpected exception: {msg!r}")
    loop.create_task(shutdown(loop))


def main():
    setup()
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for sig in signals:
        handler = partial(asyncio.create_task, shutdown(loop, sig))
        loop.add_signal_handler(sig, handler)
    loop.set_exception_handler(handle_exception)

    try:
        loop.create_task(run_strategy(configs.STRATEGY))
        loop.run_forever()
    finally:
        loop.close()
        logging.info("Successfully shutdown")


if __name__ == "__main__":
    main()
