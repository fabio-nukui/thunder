from __future__ import annotations

import asyncio
import logging
from enum import Enum, auto
from typing import TYPE_CHECKING, AsyncIterable, Mapping, TypeVar

import httpx

import configs
import utils
from utils.cache import CacheGroup, ttl_cache

from ..tx_filter import Filter
from . import utils_rpc
from .base_api import Api

if TYPE_CHECKING:
    from .async_client import TerraClient

log = logging.getLogger(__name__)

DECODER_CACHE_SIZE = 2000
DECODER_CACHE_TTL = 60
DECODE_TX_TIMEOUT = 0.5
MAX_RAW_TX_LENGTH = 3000
_T = TypeVar("_T")


class DecodeError(Exception):
    pass


class UpdateEvent(Enum):
    new_block = auto()
    mempool = auto()


class MempoolCacheManager:
    def __init__(self, client: TerraClient):
        self.client = client

        self._height = 0
        self._txs_cache: dict[str, dict] = {}
        self._read_txs: set[str] = set()

    @property
    def height(self) -> int:
        return self._height

    @height.setter
    def height(self, value: int):
        self._height = value
        self._txs_cache = {}
        self._read_txs = set()

    def start(self):
        self._height = self.client.height
        self._update_task = asyncio.create_task(self._update_height())
        self._update_task.add_done_callback(utils.async_.raise_task_exception)

    def stop(self):
        self._update_task.cancel()

    async def _update_height(self):
        async for height in utils_rpc.loop_latest_height(self.client.rpc_websocket_uri):
            self.height = height

    async def filter_new_height_mempool(
        self,
        height: int,
        filters: Mapping[_T, Filter],
        new_block_only: bool = False,
    ) -> tuple[int, dict[_T, list[list[dict]]]]:
        while True:
            new_height, mempool = await self.get_new_height_mempool(height, new_block_only)
            filtered_mempool = {
                key: list_msgs
                for key, filter_ in filters.items()
                if (list_msgs := [msgs for msgs in mempool if filter_.match_msgs(msgs)])
            }
            if new_height > height or filtered_mempool:
                return new_height, filtered_mempool

    async def get_new_height_mempool(
        self,
        height: int,
        new_block_only: bool,
    ) -> tuple[int, list[list[dict]]]:
        task_wait_next_block = asyncio.create_task(self._wait_next_block(height))
        task_mempool_txs = asyncio.create_task(self._update_mempool_txs(wait_for_changes=True))

        tasks = asyncio.as_completed([task_wait_next_block, task_mempool_txs])
        event = await next(tasks)
        if new_block_only and event != UpdateEvent.new_block:
            await next(tasks)
        else:
            task_wait_next_block.cancel()

        unread_txs_msgs = [
            tx["msg"] for key, tx in self._txs_cache.items() if key not in self._read_txs
        ]
        self._read_txs = set(self._txs_cache)
        return self.height, unread_txs_msgs

    async def _wait_next_block(self, min_height: int) -> UpdateEvent:
        while True:
            if self.height > min_height:
                return UpdateEvent.new_block
            await asyncio.sleep(configs.TERRA_POLL_INTERVAL)

    async def _update_mempool_txs(self, wait_for_changes: bool) -> UpdateEvent:
        while True:
            res = await self.client.rpc_http_client.get("unconfirmed_txs")
            raw_txs: set[str] = {
                tx for tx in res.json()["result"]["txs"] if len(tx) < MAX_RAW_TX_LENGTH
            }
            set_cache = set(self._txs_cache)
            if not wait_for_changes or raw_txs != set_cache:
                break
            await asyncio.sleep(configs.TERRA_POLL_INTERVAL)

        if not set_cache < raw_txs:
            # Some txs were removed from mempool, a new block has arrived
            self._txs_cache = {}
            self._read_txs = set()

        tasks = {
            raw_tx: self._decode_tx(raw_tx)
            for raw_tx in raw_txs
            if raw_tx not in self._txs_cache
        }
        try:
            txs = await asyncio.gather(*tasks.values())
        except Exception as e:
            e.args = (*e.args, f"{len(tasks)=}")
            raise e
        self._txs_cache.update({raw_tx: tx for raw_tx, tx in zip(tasks, txs) if tx})
        return UpdateEvent.mempool

    async def fetch_mempool_txs(self) -> dict[str, dict]:
        await self._update_mempool_txs(wait_for_changes=False)
        return self._txs_cache

    @ttl_cache(CacheGroup.TERRA, maxsize=DECODER_CACHE_SIZE, ttl=DECODER_CACHE_TTL)
    async def _decode_tx(self, raw_tx: str) -> dict:
        try:
            response = await self.client.lcd_http_client.post(
                "txs/decode",
                json={"tx": raw_tx},
                timeout=DECODE_TX_TIMEOUT,
                follow_redirects=True,
                n_tries=1,
                supress_logs=True,
            )
        except httpx.HTTPError as e:
            if isinstance(e, httpx.HTTPStatusError) and "not support" in e.response.text:
                # Non legacy-compatible txs
                return {}
            log.debug(f"Decode error {len(raw_tx)=}: {raw_tx=}")
            return {}
        else:
            return response.json()["result"]


class MempoolApi(Api):
    def __init__(self, client: "TerraClient"):
        super().__init__(client)
        self._cache_manager = MempoolCacheManager(client)

    async def fetch_mempool_msgs(self) -> list[list[dict]]:
        txs = await self._cache_manager.fetch_mempool_txs()
        return [tx["msg"] for tx in txs.values()]

    def start(self):
        self._cache_manager.start()

    def stop(self):
        self._cache_manager.stop()

    async def get_height_mempool(self, height: int) -> tuple[int, list[list[dict]]]:
        return await self._cache_manager.get_new_height_mempool(height, new_block_only=False)

    async def iter_height_mempool(
        self,
        filters: Mapping[_T, Filter],
    ) -> AsyncIterable[tuple[int, dict[_T, list[list[dict]]]]]:
        while True:
            last_height, mempool = await self._cache_manager.filter_new_height_mempool(
                self.client.height, filters, new_block_only=False
            )
            if last_height > self.client.height:
                self.client.height = last_height
            yield last_height, mempool