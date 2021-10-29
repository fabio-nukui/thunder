from __future__ import annotations

import asyncio
import logging
from asyncio.futures import Future
from typing import TYPE_CHECKING, AsyncIterable, Mapping, TypeVar

import httpx

import configs
import utils
from exceptions import BlockchainNewState

from ..tx_filter import Filter
from . import utils_rpc
from .base_api import Api

if TYPE_CHECKING:
    from .async_client import TerraClient

log = logging.getLogger(__name__)

MAX_CONCURRENT_DECODE_REQUESTS = 10
MAX_DECODER_ERRORS_PER_BLOCK = 20
DECODE_TX_TIMEOUT = 0.1
_T = TypeVar("_T")


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
        self._rpc_http_uri = rpc_http_uri
        self._lcd_uri = lcd_uri

        self._txs_cache: dict[str, dict] = {}
        self._read_txs: set[str] = set()
        self._running_thread_update_height = False
        self.new_blockchain_state = False
        self._decoder_error_counter = 0
        self._decoder_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DECODE_REQUESTS)

    async def start(self):
        self._rpc_client = utils.ahttp.AsyncClient(base_url=self._rpc_http_uri)
        self._lcd_client = utils.ahttp.AsyncClient(base_url=self._lcd_uri, n_tries=1)

    async def close(self):
        await asyncio.gather(
            self._lcd_client.aclose(),
            self._rpc_client.aclose(),
        )

    @property
    def height(self) -> int:
        return self._height

    @height.setter
    def height(self, value: int):
        self._height = value
        self.new_blockchain_state = False
        self._decoder_error_counter = 0

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
        cor_next_height = utils_rpc.wait_next_block_height(self._rpc_websocket_uri)
        cor_mempool_txs = self.fetch_mempool_txs()
        if height != self.height or new_block_only:
            self.height = await cor_next_height
            self._txs_cache = await cor_mempool_txs
        elif self._running_thread_update_height:
            self._txs_cache = await cor_mempool_txs
            del cor_mempool_txs
        else:
            as_completed_events = asyncio.as_completed((cor_next_height, cor_mempool_txs))
            data = await next(as_completed_events)
            if isinstance(data, int):
                self.height = data
                del as_completed_events
            else:
                self._txs_cache = data  # type: ignore
                self._running_thread_update_height = True
                fut_next_height: Future[int] = next(as_completed_events)  # type: ignore
                asyncio.create_task(self._update_height(fut_next_height))
        if self.new_blockchain_state:
            raise BlockchainNewState
        unread_txs_msgs = [
            tx["msg"] for key, tx in self._txs_cache.items() if key not in self._read_txs
        ]
        self._read_txs = set(self._txs_cache)
        return self.height, unread_txs_msgs

    async def fetch_mempool_txs(self) -> dict[str, dict]:
        n_txs = len(self._txs_cache)
        while True:
            res = await self._rpc_client.get("unconfirmed_txs")
            raw_txs: list[str] = res.json()["result"]["txs"]
            if n_txs != len(raw_txs):
                break
            await asyncio.sleep(configs.TERRA_POLL_INTERVAL)

        if not self._read_txs.issubset(raw_txs):
            # Some txs were removed from mempool, a new block has arrived
            self.new_blockchain_state = True
            self._txs_cache = {key: tx for key, tx in self._txs_cache.items() if key in raw_txs}
            self._read_txs = set()

        tasks = {
            raw_tx: self._decode_tx(raw_tx) for raw_tx in raw_txs if raw_tx not in self._txs_cache
        }
        try:
            txs = await asyncio.gather(*tasks.values())
        except Exception as e:
            e.args = (*e.args, f"{len(tasks)=}")
            raise e
        new_txs = {raw_tx: tx for raw_tx, tx in zip(tasks, txs) if tx}
        return self._txs_cache | new_txs

    async def _decode_tx(self, raw_tx: str) -> dict:
        try:
            async with self._decoder_semaphore:
                response = await self._lcd_client.post(
                    "txs/decode", json={"tx": raw_tx}, timeout=DECODE_TX_TIMEOUT
                )
        except httpx.HTTPError:
            self._decoder_error_counter += 1
            if self._decoder_error_counter > MAX_DECODER_ERRORS_PER_BLOCK:
                raise Exception(f"{self._decoder_error_counter} decoder errors last block")
            return {}
        else:
            return response.json()["result"]

    async def _update_height(self, fut_next_height: Future[int]):
        self.height = await fut_next_height
        self._running_thread_update_height = False


class MempoolApi(Api):
    def __init__(self, client: "TerraClient"):
        super().__init__(client)
        self.new_block_only = False
        self._rpc_websocket_uri = str(client.rpc_http_uri)

        self._cache_manager = MempoolCacheManager(
            client.height,
            client.rpc_websocket_uri,
            self._rpc_websocket_uri,
            str(client.lcd_uri),
        )

    async def start(self):
        await self._cache_manager.start()

    async def close(self):
        await self._cache_manager.close()

    async def fetch_mempool_msgs(self) -> list[list[dict]]:
        txs = await self._cache_manager.fetch_mempool_txs()
        if self._cache_manager.new_blockchain_state:
            raise BlockchainNewState
        return [tx["msg"] for tx in txs.values()]

    async def get_height_mempool(self, height: int) -> tuple[int, list[list[dict]]]:
        return await self._cache_manager.get_new_height_mempool(height, new_block_only=False)

    async def iter_height_mempool(
        self,
        filters: Mapping[_T, Filter],
    ) -> AsyncIterable[tuple[int, dict[_T, list[list[dict]]]]]:
        while True:
            try:
                last_height, mempool = await self._cache_manager.filter_new_height_mempool(
                    self.client.height, filters, self.new_block_only
                )
            except BlockchainNewState:
                continue
            if last_height > self.client.height:
                self.client.height = last_height
            yield last_height, mempool
