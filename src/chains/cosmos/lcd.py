import asyncio
import logging

import aiohttp
from aiohttp.client_exceptions import ServerDisconnectedError
from terra_sdk.client.lcd import AsyncLCDClient as TerraAsyncLCDClient

log = logging.getLogger(__name__)

DEFAULT_MAX_CONCURRENT_REQUESTS = 20


class AsyncLCDClient(TerraAsyncLCDClient):
    def __init__(
        self,
        *args,
        max_concurrent_requests: int = DEFAULT_MAX_CONCURRENT_REQUESTS,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._reconnect_lock = asyncio.Lock()

    async def _get(self, *args, **kwargs):
        try:
            async with self._semaphore:
                log.debug(f"LCD semaphore: {self._semaphore._value}, {args=}")
                return await super()._get(*args, **kwargs)
        except (ServerDisconnectedError, RuntimeError):
            await self._reconnect_session()
            return await super()._get(*args, **kwargs)

    async def _post(self, *args, **kwargs):
        try:
            async with self._semaphore:
                log.debug(f"LCD semaphore: {self._semaphore._value}, {args=}")
                return await super()._post(*args, **kwargs)
        except (ServerDisconnectedError, RuntimeError):
            await self._reconnect_session()
            return await super()._post(*args, **kwargs)

    async def _reconnect_session(self):
        async with self._reconnect_lock:
            if not self.session.closed:
                return
            log.debug("Reconnecting LCD session")
            old_session = self.session
            self.session = aiohttp.ClientSession(
                headers={"Accept": "application/json"}, loop=self.loop
            )
            await old_session.close()
