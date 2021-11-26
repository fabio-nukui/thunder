from __future__ import annotations

import asyncio
import logging

import utils
from utils.ahttp import AsyncClient

log = logging.getLogger(__name__)

_MAX_BROADCASTER_HEIGHT_DIFFERENCE = 2


class BroadcasterMixin:
    height: int

    def __init__(
        self,
        use_broadcaster: bool,
        broadcaster_uris: list[str],
        broadcast_lcd_uris: list[str],
        *args,
        **kwargs,
    ):
        self.use_broadcaster = use_broadcaster
        self.broadcaster_uris = broadcaster_uris
        self.broadcast_lcd_uris = broadcast_lcd_uris

        self._broadcaster_clients: list[AsyncClient] = []
        self._broadcasters_status: dict[AsyncClient, bool] = {}
        self.broadcast_lcd_clients: list[AsyncClient] = []
        self.active_broadcaster: AsyncClient | None = None

        super().__init__(*args, **kwargs)  # type: ignore

    async def _init_broadcaster_clients(self):
        await self._fix_broadcaster_urls()
        self._broadcaster_clients = [
            utils.ahttp.AsyncClient(base_url=url, verify=False) for url in self.broadcaster_uris
        ]
        self._broadcasters_status = {c: False for c in self._broadcaster_clients}
        self.broadcast_lcd_clients = [
            utils.ahttp.AsyncClient(base_url=url) for url in self.broadcast_lcd_uris
        ]

    async def _fix_broadcaster_urls(self):
        host_ip = await utils.ahttp.get_host_ip()
        self.broadcaster_uris = [
            url.replace(host_ip, "localhost") for url in self.broadcaster_uris
        ]
        self.broadcast_lcd_uris = [url for url in self.broadcast_lcd_uris if host_ip not in url]

    async def update_active_broadcaster(self):
        tasks = (self._set_broadcaster_status(c) for c in self._broadcaster_clients)
        await asyncio.gather(*tasks)

        n_ok = sum(self._broadcasters_status.values())
        n_total = len(self._broadcasters_status)
        log.debug(f"{n_ok}/{n_total} broadcasters OK")

        if self.use_broadcaster and not n_ok:
            log.info("Stop using broadcaster")
            self.use_broadcaster = False
        elif not self.use_broadcaster and n_ok:
            log.info("Start using broadcaster")
            self.use_broadcaster = True

        if self.use_broadcaster:
            for client, status_ok in self._broadcasters_status.items():
                if status_ok:
                    if self.active_broadcaster != client:
                        log.info(f"Switching broadcaster to {client.base_url}")
                        self.active_broadcaster = client
                    return

    async def _set_broadcaster_status(self, broadcaster_client: AsyncClient):
        try:
            res = await broadcaster_client.get("lcd/blocks/latest", supress_logs=True)
            height = int(res.json()["block"]["header"]["height"])
            if self.height - height > _MAX_BROADCASTER_HEIGHT_DIFFERENCE:
                raise Exception(f"Broadcaster {height=} behind {self.height=}")
        except Exception as e:
            previous_status = self._broadcasters_status.get(broadcaster_client)
            if previous_status or previous_status is None:
                log.debug(f"Error with broadcaster={broadcaster_client.base_url}: {e!r}")
                self._broadcasters_status[broadcaster_client] = False
        else:
            self._broadcasters_status[broadcaster_client] = True
