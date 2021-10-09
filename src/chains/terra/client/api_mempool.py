from __future__ import annotations

import json
import logging
import threading
import time

import httpx

import configs
import utils

from ..core import BaseMempoolApi

log = logging.getLogger(__name__)


class MempoolCacheManager(threading.Thread):
    def __init__(self, fcd_client: utils.http.Client):
        super().__init__(daemon=True)
        self._client = fcd_client

        self._txs_cache: dict[str, dict] = {}
        self._read_txs: dict[str, dict] = {}
        self._mempool_bytes = 0
        self.height = 0

    def run(self):
        self._update_height()
        while True:
            mempool_txs = self._fetch_mempool_txs()
            stale_tx_cache = not (self._txs_cache.keys() <= mempool_txs.keys())
            if stale_tx_cache:
                self._txs_cache = {}
                self._read_txs = {}
                self._update_height()
            self._txs_cache = mempool_txs
            time.sleep(configs.TERRA_POLL_INTERVAL)

    def get_txs(self, height: int) -> dict[str, dict]:
        if height > self.height:
            return {}
        unread_tx_hashes = self._txs_cache.keys() - self._read_txs.keys()
        unread_txs = {key: self._txs_cache[key] for key in unread_tx_hashes}
        self._read_txs |= unread_txs
        return unread_txs

    def _update_height(self):
        res = self._client.get("blocks/latest")
        res.raise_for_status()
        self.height = int(res.json()["block"]["header"]["height"])

    def _fetch_mempool_txs(self) -> dict[str, dict]:
        with self._client.stream("GET", "v1/mempool") as res:
            res.raise_for_status()
            n_bytes_response = int(res.headers["Content-Length"])

            # Assume mempool has not changed if response has the same number of bytes as last time
            if self._mempool_bytes == n_bytes_response:
                return self._txs_cache
            self._mempool_bytes = n_bytes_response
            data = json.loads(res.read())
        return {tx["txhash"]: tx["tx"]["value"] for tx in data["txs"]}


class MempoolApi(BaseMempoolApi):
    def start_cache(self):
        self._cache_manager = MempoolCacheManager(self.client.fcd_client)
        self._cache_manager.start()

    def get_new_txs(self, height: int) -> dict[str, dict]:
        return self._cache_manager.get_txs(height)

    @property
    def height(self) -> int:
        return self._cache_manager.height
