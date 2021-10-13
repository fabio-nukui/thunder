import logging

from cachetools import TTLCache

log = logging.getLogger(__name__)


class TTLCacheStats(TTLCache):
    def __init__(self, *args, **kwargs):
        """Time-to-live cache that saves statistics on cache hit/miss"""
        super().__init__(*args, **kwargs)

        self._n_hit = 0
        self._n_miss = 0
        self._n_hit_total = 0
        self._n_miss_total = 0

    def clear(self):
        super().clear()
        self._n_hit = 0
        self._n_miss = 0

    def __getitem__(self, key):
        hit = super().__getitem__(key)
        self._n_hit += 1
        self._n_hit_total += 1
        log.debug(f"Cache hit: {key}, {hit}")

        return hit

    def __setitem__(self, k, v):
        super().__setitem__(k, v)
        self._n_miss += 1
        self._n_miss_total += 1
        log.debug(f"Cache set: {k}, {v}")

    def setdefault(self, k, v):
        self._n_miss += 1
        self._n_miss_total += 1
        log.debug(f"Cache set: {k}, {v}")

        return super().setdefault(k, v)
