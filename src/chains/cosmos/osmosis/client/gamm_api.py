from __future__ import annotations

import asyncio

from cosmos_proto.cosmos.base.query.v1beta1 import PageRequest
from cosmos_proto.osmosis.gamm.v1beta1 import MsgStub, Pool, PoolAsset, QueryStub
from cosmos_proto.osmosis.gamm.v1beta1 import SwapAmountInRoute as SwapAmountInRoute_pb
from cosmos_sdk.core import AccAddress, Coin

from ..data.gamm import MsgSwapExactAmountIn, SwapAmountInRoute
from ..token import OsmosisNativeToken, OsmosisTokenAmount
from .base_api import Api


class GammApi(Api):
    def start(self):
        self.grpc_msgs = MsgStub(self.client.grpc_channel)
        self.grpc_query = QueryStub(self.client.grpc_channel)

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
        tasks = (
            self.grpc_query.estimate_swap_exact_amount_in(
                sender=sender,
                pool_id=pool_id,
                token_in=amount_in.to_str(),
                routes=[SwapAmountInRoute_pb(pool_id, token_out.denom)],
            )
            for pool_id, pool in pools.items()
            if len(set_denoms & {a.token.denom for a in pool.pool_assets}) == 2
        )
        res = await asyncio.gather(*tasks)
        amounts = (token_out.to_amount(int_amount=r.token_out_amount) for r in res)
        return max(amounts, key=lambda x: x.amount)
