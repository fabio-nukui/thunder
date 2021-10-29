import asyncio
import logging

import aiohttp
from aiohttp.client_exceptions import ServerDisconnectedError
from terra_sdk.client.lcd import AsyncLCDClient

log = logging.getLogger(__name__)

MAX_CONCURRENT_REQUESTS = 20


class AsyncLCDClient2(AsyncLCDClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._requests_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self._reconnect_lock = asyncio.Lock()

    async def _get(self, *args, **kwargs):
        try:
            async with self._requests_semaphore:
                return await super()._get(*args, **kwargs)
        except ServerDisconnectedError:
            await self._reconnect_session()
            return await super()._get(*args, **kwargs)

    async def _post(self, *args, **kwargs):
        try:
            async with self._requests_semaphore:
                return await super()._post(*args, **kwargs)
        except ServerDisconnectedError:
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
