from __future__ import annotations

from osmosis_proto.cosmos.base.query.v1beta1 import PageRequest
from osmosis_proto.osmosis.gamm.v1beta1 import Pool, PoolAsset

from .base_api import Api


class GammApi(Api):
    async def get_all_pools(self) -> list[Pool]:
        pools: list[Pool] = []
        next_key = b""
        while True:
            req = PageRequest(key=next_key)
            res = await self.client.grpc_gamm.pools(pagination=req)
            pools.extend([Pool.FromString(p.value) for p in res.pools])
            if not (next_key := res.pagination.next_key):
                break
        return pools

    async def get_pool(self, pool_id: int) -> Pool:
        res = await self.client.grpc_gamm.pool(pool_id=pool_id)
        return Pool.FromString(res.pool.value)

    async def get_pool_assets(self, pool_id: int) -> list[PoolAsset]:
        res = await self.client.grpc_gamm.pool_assets(pool_id=pool_id)
        return res.pool_assets
