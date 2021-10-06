from __future__ import annotations

import json
import logging
from collections import defaultdict
from enum import Enum
from typing import Callable, Optional

from cachetools import TTLCache, cached
from cachetools.keys import _HashedTuple  # type: ignore

import configs

log = logging.getLogger(__name__)

_caches: dict[CacheGroup, list[TTLCache | TTLCacheStats]] = defaultdict(list)


class CacheGroup(Enum):
    ALL = "all"
    DEFAULT = "default"
    BSC = "bsc"
    ETHEREUM = "ethereum"
    TERRA = "terra"


CACHE_GROUPS_TTL = {
    CacheGroup.DEFAULT: configs.DEFAULT_CACHE_TTL,
    CacheGroup.BSC: configs.BSC_CACHE_TTL,
    CacheGroup.ETHEREUM: configs.ETHEREUM_CACHE_TTL,
    CacheGroup.TERRA: configs.TERRA_CACHE_TTL,
}

CACHE_GROUPS_TTL[CacheGroup.ALL] = min(CACHE_GROUPS_TTL.values())


class _HashedTupleJSON(_HashedTuple):
    """Use JSON serialization as fallback for non-hashable arguments in caches"""

    __hashvalue = None

    def __hash__(self, hash=tuple.__hash__):
        hashvalue = self.__hashvalue
        if hashvalue is None:
            try:
                hashvalue = hash(self)
            except TypeError:
                args = []
                for arg in self:
                    try:
                        hash((arg,))
                    except TypeError:
                        args.append(json.dumps(arg))
                    else:
                        args.append(arg)
                hashvalue = hash(_HashedTupleJSON(args))
            self.__hashvalue = hashvalue
        return hashvalue


_kwmark = (_HashedTupleJSON,)


def _hashkey_json(*args, **kwargs):
    if kwargs:
        return _HashedTupleJSON(args + sum(sorted(kwargs.items()), _kwmark))
    else:
        return _HashedTupleJSON(args)


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


def _get_ttl_cache(group: CacheGroup, maxsize: int, ttl: float = None) -> TTLCache | TTLCacheStats:
    ttl = CACHE_GROUPS_TTL[group] if ttl is None else ttl
    cache = TTLCacheStats(maxsize, ttl) if configs.CACHE_STATS else TTLCache(maxsize, ttl)

    _caches[group].append(cache)
    return cache


def ttl_cache(
    group: CacheGroup | Callable = CacheGroup.DEFAULT,
    maxsize: int = 100,
    ttl: Optional[int | float] = None,
):
    """TTL cache decorator with safe global clear function"""
    if callable(group):
        # ttl_cache was applied directly
        func = group
        cache = _get_ttl_cache(CacheGroup.DEFAULT, maxsize, ttl)
        return cached(cache, key=_hashkey_json)(func)
    if isinstance(group, CacheGroup):
        cache = _get_ttl_cache(group, maxsize, ttl)
        return cached(cache, key=_hashkey_json)
    raise TypeError("Expected first argument to be a CacheGroup or a callable")


def clear_caches(
    group: CacheGroup = CacheGroup.DEFAULT,
    ttl_treshold: Optional[int | float] = None,
    clear_all: bool = False,
):
    ttl_treshold = CACHE_GROUPS_TTL[group] if ttl_treshold is None else ttl_treshold
    if group == CacheGroup.ALL:
        caches_clear = [cache for list_caches in _caches.values() for cache in list_caches]
    else:
        caches_clear = _caches[group]
    for cache in caches_clear:
        if cache._TTLCache__ttl <= ttl_treshold or clear_all:  # type: ignore
            cache.clear()


def get_stats():
    if not configs.CACHE_STATS:
        raise Exception("Stats only available if configs.CACHE_STATS=True")
    all_caches = [
        cache
        for list_caches in _caches.values()
        for cache in list_caches
        if isinstance(cache, TTLCacheStats)
    ]
    return {
        "n_hit": sum(cache._n_hit for cache in all_caches),
        "n_miss": sum(cache._n_miss for cache in all_caches),
        "n_hit_total": sum(cache._n_hit_total for cache in all_caches),
        "n_miss_total": sum(cache._n_hit_total for cache in all_caches),
    }
