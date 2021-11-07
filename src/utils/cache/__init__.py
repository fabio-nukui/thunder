from __future__ import annotations

import logging
from collections import defaultdict
from enum import Enum
from typing import Callable, Optional

from cachetools import LRUCache, TTLCache

import configs

from .asyncache import cached
from .json_hash_key import json_hashkey
from .ttl_cache_stats import TTLCacheStats

log = logging.getLogger(__name__)

_caches: dict[CacheGroup, list[TTLCache | TTLCacheStats | LRUCache]] = defaultdict(list)


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


def _get_ttl_cache(
    group: CacheGroup,
    maxsize: int,
    ttl: float = None,
) -> TTLCache | TTLCacheStats:
    ttl = CACHE_GROUPS_TTL[group] if ttl is None else ttl
    cache = TTLCacheStats(maxsize, ttl) if configs.CACHE_STATS else TTLCache(maxsize, ttl)

    _caches[group].append(cache)
    return cache


def lru_cache(maxsize: int | Callable = 100):
    """LRU cache decorator for sync and async functions"""
    if callable(maxsize):
        # ttl_cache was applied directly
        func = maxsize
        cache: LRUCache = LRUCache(100)
        _caches[CacheGroup.DEFAULT].append(cache)
        return cached(cache, key=json_hashkey)(func)
    if isinstance(maxsize, int):
        cache = LRUCache(maxsize)
        _caches[CacheGroup.DEFAULT].append(cache)
        return cached(cache, key=json_hashkey)
    raise TypeError("Expected first argument to be an int or a callable")


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
        return cached(cache, key=json_hashkey)(func)
    if isinstance(group, CacheGroup):
        cache = _get_ttl_cache(group, maxsize, ttl)
        return cached(cache, key=json_hashkey)
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
