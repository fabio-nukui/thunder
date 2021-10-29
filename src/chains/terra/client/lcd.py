import logging

import aiohttp
from aiohttp.client_exceptions import ServerDisconnectedError
from terra_sdk.client.lcd import AsyncLCDClient

log = logging.getLogger(__name__)


class AsyncLCDClient2(AsyncLCDClient):
    async def _get(self, *args, **kwargs):
        try:
            return await super()._get(*args, **kwargs)
        except ServerDisconnectedError:
            await self._reconnect_session()
            return await super()._get(*args, **kwargs)

    async def _post(self, *args, **kwargs):
        try:
            return await super()._post(*args, **kwargs)
        except ServerDisconnectedError:
            await self._reconnect_session()
            return await super()._post(*args, **kwargs)

    async def _reconnect_session(self):
        log.debug("Reconnecting LCD session")
        old_session = self.session
        self.session = aiohttp.ClientSession(headers={"Accept": "application/json"}, loop=self.loop)
        await old_session.close()
