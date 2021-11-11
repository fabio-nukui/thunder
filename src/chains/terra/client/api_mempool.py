from __future__ import annotations

import asyncio
import logging
import threading
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
    null = auto()


class LatestHeightThread(threading.Thread):
    def __init__(self, mcm: MempoolCacheManager, *args, daemon: bool = True, **kargs):
        super().__init__(*args, daemon=daemon, **kargs)

        self.mcm = mcm
        self._loop = asyncio.new_event_loop()
        self._stopped = threading.Event()

    def run(self):
        self._loop.create_task(self._update_height())
        self._loop.run_forever()

    def stop(self):
        self._stopped.set()
        self._loop.create_task(utils.async_.stop_loop(self._loop))

    async def _update_height(self):
        async for height in utils_rpc.loop_latest_height(self.mcm.client.rpc_websocket_uri):
            self.mcm.height = height
            if self._stopped.is_set():
                return


class MempoolCacheManager:
    def __init__(self, client: TerraClient):
        self.client = client

        self._txs_cache: dict[str, dict] = {}
        self._read_txs: set[str] = set()

        self._height = 0
        self._height_thread = LatestHeightThread(self)
        self._stop_tasks = False

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
        self._height_thread.start()

    def stop(self):
        self._height_thread.stop()

    async def filter_new_height_mempool(
        self,
        height: int,
        filters: Mapping[_T, Filter],
        new_block_only: bool = False,
    ) -> tuple[int, dict[_T, list[list[dict]]]]:
        while True:
            new_height, mempool = await self.get_new_height_mempool(height, new_block_only)
            filtered_mempool = {
                key: [msgs for msgs in mempool if filter_.match_msgs(msgs)]
                for key, filter_ in filters.items()
            }
            any_filtered_msg = any(list_msgs for list_msgs in filtered_mempool.values())
            if new_height > height or any_filtered_msg:
                return new_height, filtered_mempool

    async def get_new_height_mempool(
        self,
        height: int,
        new_block_only: bool,
    ) -> tuple[int, list[list[dict]]]:
        cor_wait_next_block = self._wait_next_block(height)
        cor_mempool_txs = self._update_mempool_txs(wait_for_changes=True)
        self._stop_tasks = False
        tasks = asyncio.as_completed((cor_wait_next_block, cor_mempool_txs))
        event = await next(tasks)
        if new_block_only and event != UpdateEvent.new_block:
            await next(tasks)
        self._stop_tasks = True
        del tasks
        del cor_wait_next_block
        del cor_mempool_txs
        unread_txs_msgs = [
            tx["msg"] for key, tx in self._txs_cache.items() if key not in self._read_txs
        ]
        self._read_txs = set(self._txs_cache)
        return self.height, unread_txs_msgs

    async def _wait_next_block(self, min_height: int) -> UpdateEvent:
        while True:
            if self._stop_tasks:
                return UpdateEvent.null
            if self.height > min_height:
                return UpdateEvent.new_block
            await asyncio.sleep(configs.TERRA_POLL_INTERVAL)

    async def _update_mempool_txs(self, wait_for_changes: bool) -> UpdateEvent:
        while True:
            if self._stop_tasks:
                return UpdateEvent.null
            res = await self.client.rpc_http_client.get("unconfirmed_txs")
            raw_txs: list[str] = [
                tx for tx in res.json()["result"]["txs"] if len(tx) < MAX_RAW_TX_LENGTH
            ]
            if not wait_for_changes or set(raw_txs) != set(self._txs_cache):
                break
            await asyncio.sleep(configs.TERRA_POLL_INTERVAL)

        if not set(self._txs_cache).issubset(raw_txs):
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
        self._stop_tasks = False
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
