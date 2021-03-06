import asyncio
import importlib
import logging
import signal
from types import ModuleType

import configs
import utils
from exceptions import NodeSyncing
from startup import setup

log = logging.getLogger(__name__)

GIT_COMMIT = open("git_commit").read().strip()


async def run_strategy(strategy_module: ModuleType):
    while True:
        try:
            log.info(f"Starting strategy {strategy_module}")
            await strategy_module.run()  # type: ignore
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
        log.info(f"Received exit signal {signal.name}", extra={"_sync": True})
    log.info("Shutting down", extra={"_sync": True})
    await utils.async_.stop_loop(loop)


def handle_exception(loop: asyncio.AbstractEventLoop, context: dict):
    log.error(f"Unexpected exception: {context=}", extra={"_sync": True})
    if not loop.is_closed():
        loop.create_task(shutdown(loop))


def get_event_loop() -> asyncio.AbstractEventLoop:
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(loop, s)))
    loop.set_exception_handler(handle_exception)
    return loop


def main():
    setup()
    log.info(f"Running on git commit {GIT_COMMIT}")
    strategy_module = importlib.import_module(f"strategies.{configs.STRATEGY}")

    loop = get_event_loop()
    try:
        loop.create_task(run_strategy(strategy_module))
        loop.run_forever()
    finally:
        loop.close()
        log.info("Successfully shutdown", extra={"_sync": True})


if __name__ == "__main__":
    main()
