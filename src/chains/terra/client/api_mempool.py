from __future__ import annotations

import asyncio
import json
import logging
from asyncio.futures import Future
from typing import Iterator

import configs
import utils
from exceptions import BlockchainNewState

from ..core import BaseMempoolApi, BaseTerraClient
from . import utils_rpc

log = logging.getLogger(__name__)


class MempoolCacheManager:
    def __init__(
        self,
        height: int,
        rpc_websocket_uri: str,
        rpc_http_uri: str,
        lcd_uri: str,
    ):
        self._height = height
        self._rpc_websocket_uri = rpc_websocket_uri
        self._rpc_client = utils.ahttp.AsyncClient(base_url=rpc_http_uri)
        self._lcd_client = utils.ahttp.AsyncClient(base_url=lcd_uri)

        self._txs_cache: dict[str, dict] = {}
        self._read_txs: set[str] = set()
        self._updating_height = False
        self._waiting_new_block = False

    async def get_new_height_mempool(self, height: int) -> tuple[int, list[dict]]:
        cor_mempool_txs = self._fetch_mempool_txs()
        if height == self._height and self._updating_height:
            self._txs_cache = await cor_mempool_txs
        else:
            cor_next_height = utils_rpc.wait_next_block_height(self._rpc_websocket_uri)
            events = (cor_next_height, cor_mempool_txs)
            if height != self._height:
                self._height, self._txs_cache = await asyncio.gather(*events)
            else:  # height == self._height and not self._updating_height
                as_completed_events = asyncio.as_completed(events)
                data = await next(as_completed_events)
                if isinstance(data, int):
                    self._height = data
                    self._waiting_new_block = False
                    del as_completed_events
                else:
                    self._txs_cache = data
                    self._updating_height = True
                    fut_next_height: Future[int] = next(as_completed_events)  # type: ignore
                    asyncio.run_coroutine_threadsafe(
                        self._ensure_height(fut_next_height), asyncio.get_event_loop()
                    )
        if self._waiting_new_block:
            raise BlockchainNewState
        unread_txs = [tx for key, tx in self._txs_cache.items() if key not in self._read_txs]
        self._read_txs = set(self._txs_cache)
        return self._height, unread_txs

    async def _fetch_mempool_txs(self) -> dict[str, dict]:
        n_txs = len(self._txs_cache)
        while True:
            res = await self._rpc_client.get("unconfirmed_txs")
            raw_txs = json.loads(await res.aread())["result"]["txs"]
            if len(raw_txs) != n_txs:
                break
            await asyncio.sleep(configs.TERRA_POLL_INTERVAL)

        if not self._read_txs.issubset(raw_txs):
            # Some txs were removed from mempool, a new block has arrived
            self._waiting_new_block = True
            self._txs_cache = {key: tx for key, tx in self._txs_cache.items() if key in raw_txs}
            self._read_txs = set()

        tasks = {
            raw_tx: self._lcd_client.post("txs/decode", json={"tx": raw_tx})
            for raw_tx in raw_txs
            if raw_tx not in self._txs_cache
        }
        responses = await asyncio.gather(*tasks.values())
        new_txs = {raw_tx: res.json()["result"] for raw_tx, res in zip(tasks, responses)}
        return self._txs_cache | new_txs

    async def _ensure_height(self, fut_next_height: Future[int]):
        self._height = await fut_next_height
        self._updating_height = False
        self._waiting_new_block = False


class MempoolApi(BaseMempoolApi):
    def __init__(self, client: BaseTerraClient):
        super().__init__(client)
        self._cache_manager = MempoolCacheManager(
            client.height,
            client.rpc_websocket_uri,
            str(client.rpc_http_client.base_url),
            str(client.lcd_http_client.base_url),
        )

    async def get_height_mempool(self, height: int) -> tuple[int, list[dict]]:
        return await self._cache_manager.get_new_height_mempool(height)

    async def loop_height_mempool(self) -> Iterator[tuple[int, list[dict]]]:
        while True:
            try:
                last_height, txs = await self._cache_manager.get_new_height_mempool(
                    self.client.height
                )
            except BlockchainNewState:
                continue
            if last_height > self.client.height:
                self.client.height = last_height
            yield last_height, txs
