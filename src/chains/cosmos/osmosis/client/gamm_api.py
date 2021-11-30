from __future__ import annotations

import asyncio

from cosmos_proto.cosmos.base.query.v1beta1 import PageRequest
from cosmos_proto.osmosis.gamm.v1beta1 import MsgStub, Pool, PoolAsset, QueryStub
from cosmos_sdk.core import AccAddress, Coin

from chains.cosmos.osmosis.liquidity_pair import GAMMLiquidityPool
from utils.cache import CacheGroup, ttl_cache

from ..data.gamm import MsgSwapExactAmountIn, SwapAmountInRoute
from ..token import OsmosisNativeToken, OsmosisTokenAmount
from .base_api import Api

_ALL_POOLS_CACHE_TTL = 30


class GammApi(Api):
    def start(self):
        self.grpc_msgs = MsgStub(self.client.grpc_channel)
        self.grpc_query = QueryStub(self.client.grpc_channel)

    @ttl_cache(CacheGroup.OSMOSIS, ttl=_ALL_POOLS_CACHE_TTL)
    async def get_all_pools(self) -> dict[int, Pool]:
        pools: dict[int, Pool] = {}
        next_key = b""
        while True:
            req = PageRequest(key=next_key)
            res = await self.grpc_query.pools(pagination=req)
            pools.update({(p := Pool.FromString(raw.value)).id: p for raw in res.pools})
            if not (next_key := res.pagination.next_key):
                break
        return pools

    async def get_pool(self, pool_id: int) -> Pool:
        res = await self.grpc_query.pool(pool_id=pool_id)
        return Pool.FromString(res.pool.value)

    async def get_pool_assets(self, pool_id: int) -> list[PoolAsset]:
        res = await self.grpc_query.pool_assets(pool_id=pool_id)
        return res.pool_assets

    def get_swap_exact_in_msg(
        self,
        routes: list[SwapAmountInRoute],
        amount_in: OsmosisTokenAmount,
        min_out: OsmosisTokenAmount = None,
        sender: AccAddress = None,
    ) -> MsgSwapExactAmountIn:
        sender = self.client.address if sender is None else sender
        assert isinstance(amount_in.token, OsmosisNativeToken)
        return MsgSwapExactAmountIn(
            sender=sender,
            routes=routes,
            token_in=Coin(denom=amount_in.token.denom, amount=amount_in.int_amount),
            token_out_min_amount=min_out.int_amount if min_out is not None else 0,
        )

    async def get_best_amount_out(
        self,
        amount_in: OsmosisTokenAmount,
        token_out: OsmosisNativeToken,
        max_n_hops: int = 1,
        sender: str = None,
    ) -> OsmosisTokenAmount:
        if max_n_hops > 1:
            raise NotImplementedError
        if amount_in.token == token_out:
            return amount_in
        sender = sender or self.client.address
        assert isinstance(amount_in.token, OsmosisNativeToken)
        pools = await self.get_all_pools()
        set_denoms = {amount_in.token.denom, token_out.denom}
        lps = [
            await GAMMLiquidityPool.from_proto(p, self.client)
            for p in pools.values()
            if len(set_denoms & {a.token.denom for a in p.pool_assets}) == 2
        ]
        tasks = (lp.get_amount_out_exact_in(amount_in, token_out) for lp in lps)
        amounts = await asyncio.gather(*tasks)
        return max(amounts)
