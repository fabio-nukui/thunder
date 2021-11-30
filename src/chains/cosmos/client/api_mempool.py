from __future__ import annotations

import asyncio
import base64
import logging
from enum import Enum, auto
from typing import TYPE_CHECKING, AsyncIterable, Mapping, TypeVar

from cosmos_sdk.core.tx import Tx

import configs
import utils
from utils.cache import ttl_cache

from .. import utils_rpc
from ..tx_filter import Filter
from .base_api import Api

if TYPE_CHECKING:
    from .async_client import CosmosClient

log = logging.getLogger(__name__)

_DECODER_CACHE_SIZE = 2000
_DECODER_CACHE_TTL = 60
_MAX_RAW_TX_LENGTH = 3000
_T = TypeVar("_T")


class DecodeError(Exception):
    pass


class UpdateEvent(Enum):
    new_block = auto()
    mempool = auto()


class MempoolCacheManager:
    def __init__(self, client: CosmosClient):
        self.client = client

        self._height = 0
        self._txs_cache: dict[str, Tx | None] = {}
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
        verbose_decode_warnings: bool = True,
    ) -> tuple[int, dict[_T, list[Tx]]]:
        while True:
            new_height, mempool = await self.get_new_height_mempool(
                height, new_block_only, verbose_decode_warnings
            )
            filtered_mempool = {
                key: list_tx
                for key, filter_ in filters.items()
                if (list_tx := [tx for tx in mempool if filter_.match_tx(tx)])
            }
            if new_height > height or filtered_mempool:
                return new_height, filtered_mempool

    async def get_new_height_mempool(
        self, height: int, new_block_only: bool, verbose_decode_warnings: bool = True
    ) -> tuple[int, list[Tx]]:
        task_wait_next_block = asyncio.create_task(self._wait_next_block(height))
        task_mempool_txs = asyncio.create_task(
            self._update_mempool_txs(
                wait_for_changes=True, verbose_decode_warnings=verbose_decode_warnings
            )
        )

        tasks = asyncio.as_completed([task_wait_next_block, task_mempool_txs])
        event = await next(tasks)
        if new_block_only and event != UpdateEvent.new_block:
            await task_wait_next_block
        else:
            task_wait_next_block.cancel()

        unread_txs = [
            tx
            for key, tx in self._txs_cache.items()
            if key not in self._read_txs and tx is not None
        ]
        self._read_txs = set(self._txs_cache)
        return self.height, unread_txs

    async def _wait_next_block(self, min_height: int) -> UpdateEvent:
        while True:
            if self.height > min_height:
                return UpdateEvent.new_block
            await asyncio.sleep(configs.TERRA_POLL_INTERVAL)

    async def _update_mempool_txs(
        self,
        wait_for_changes: bool,
        verbose_decode_warnings: bool,
    ) -> UpdateEvent:
        while True:
            res = await self.client.rpc_http_client.get("unconfirmed_txs")
            raw_txs: set[str] = {
                tx for tx in res.json()["result"]["txs"] if len(tx) < _MAX_RAW_TX_LENGTH
            }
            set_cache = set(self._txs_cache)
            if not wait_for_changes or raw_txs != set_cache:
                break
            await asyncio.sleep(configs.TERRA_POLL_INTERVAL)

        if not set_cache < raw_txs:
            # Some txs were removed from mempool, a new block has arrived
            self._txs_cache = {}
            self._read_txs = set()

        self._txs_cache.update(
            {
                raw_tx: self._decode_tx(raw_tx, verbose_decode_warnings)
                for raw_tx in raw_txs
                if raw_tx not in self._txs_cache
            }
        )
        return UpdateEvent.mempool

    async def fetch_mempool_txs(self, verbose_decode_warnings: bool) -> list[Tx]:
        await self._update_mempool_txs(
            wait_for_changes=False, verbose_decode_warnings=verbose_decode_warnings
        )
        return list(tx for tx in self._txs_cache.values() if tx is not None)

    @ttl_cache(maxsize=_DECODER_CACHE_SIZE, ttl=_DECODER_CACHE_TTL)
    def _decode_tx(self, raw_tx: str, verbose_decode_warnings: bool) -> Tx | None:
        try:
            return Tx.from_proto_bytes(base64.b64decode(raw_tx))
        except Exception as e:
            if verbose_decode_warnings:
                log.warning(f"Decode error ({e!r}) {len(raw_tx)=}: {raw_tx=}")
            return None


class MempoolApi(Api["CosmosClient"]):
    def __init__(self, client: CosmosClient):
        super().__init__(client)
        self._cache_manager = MempoolCacheManager(client)

    async def fetch_mempool_txs(self, verbose_decode_warnings: bool = True) -> list[Tx]:
        return await self._cache_manager.fetch_mempool_txs(verbose_decode_warnings)

    def start(self):
        self._cache_manager.start()

    def stop(self):
        self._cache_manager.stop()

    async def get_height_mempool(self, height: int) -> tuple[int, list[Tx]]:
        return await self._cache_manager.get_new_height_mempool(height, new_block_only=False)

    async def iter_height_mempool(
        self,
        filters: Mapping[_T, Filter],
        verbose_decode_warnings: bool = True,
    ) -> AsyncIterable[tuple[int, dict[_T, list[Tx]]]]:
        while True:
            last_height, mempool = await self._cache_manager.filter_new_height_mempool(
                self.client.height,
                filters,
                new_block_only=False,
                verbose_decode_warnings=verbose_decode_warnings,
            )
            if last_height > self.client.height:
                self.client.height = last_height
            yield last_height, mempool
