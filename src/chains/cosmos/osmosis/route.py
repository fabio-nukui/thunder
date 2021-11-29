from __future__ import annotations

import asyncio
from typing import Sequence

from cosmos_sdk.core import AccAddress

from chains.cosmos.osmosis.client import OsmosisClient
from chains.cosmos.osmosis.data import MsgSwapExactAmountIn

from .data import SwapAmountInRoute
from .liquidity_pair import GAMMLiquidityPool
from .token import OsmosisNativeToken, OsmosisTokenAmount


class MultiRoutes:
    def __init__(self, client: OsmosisClient, routes: Sequence[RoutePools]):
        self.client = client
        self.routes = routes

        self.pools = list(set(pool for r in routes for pool in r.pools))
        self.tokens = list(set(token for r in routes for token in r.tokens))

        assert len(set(r.is_cycle for r in routes)) == 1
        self.is_cycle = self.routes[0].is_cycle

        assert len(set(r.start_token for r in routes)) == 1
        self.start_token = self.routes[0].start_token

        self.n_routes = len(self.routes)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(tokens={self.tokens}, n_routes={self.n_routes})"


class RoutePools:
    def __init__(
        self,
        tokens: list[OsmosisNativeToken],
        pools: list[GAMMLiquidityPool],
        client: OsmosisClient,
    ):
        assert len(tokens) == len(pools) + 1

        self.tokens = tokens
        self.pools = pools
        self.client = client

        self.start_token = tokens[0]
        self.is_cycle = tokens[0] == tokens[-1]

        self.routes: list[SwapAmountInRoute] = []
        self.routes_reversed: list[SwapAmountInRoute] = []
        for pool, token_in, token_out in zip(pools, tokens[:-1], tokens[1:]):
            assert token_in in pool.tokens, f"{token_in=} not in {pool=}"
            assert token_out in pool.tokens, f"{token_out=} not in {pool=}"
            self.routes.append(SwapAmountInRoute(pool.pool_id, token_out.denom))
            self.routes_reversed.insert(0, SwapAmountInRoute(pool.pool_id, token_in.denom))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({'->'.join(token.symbol for token in self.tokens)})"

    async def op_swap_exact_in(
        self,
        amount_in: OsmosisTokenAmount,
        min_amount_out: OsmosisTokenAmount = None,
        reverse: bool = False,
        safety_margin: bool | int = True,
        sender: AccAddress = None,
    ) -> tuple[OsmosisTokenAmount, list[MsgSwapExactAmountIn]]:
        sender = self.client.address if sender is None else sender
        if min_amount_out is None:
            if self.is_cycle:
                min_amount_out = amount_in
            else:
                raise ValueError("min_amount_out is obligatory if route is not cycle")

        amount_out = await self.get_amount_out_swap_exact_in(amount_in, reverse, safety_margin)
        routes = self.routes_reversed if reverse else self.routes
        msg = self.client.gamm.get_swap_exact_in_msg(routes, amount_in, min_amount_out, sender)
        return amount_out, [msg]

    async def get_amount_out_swap_exact_in(
        self,
        amount_in: OsmosisTokenAmount,
        reverse: bool = False,
        safety_margin: bool | int = False,
    ) -> OsmosisTokenAmount:
        pools = self.pools[::-1] if reverse else self.pools
        tokens = self.tokens[-1:0:-1] if reverse else self.tokens[1:]
        for pool, token_out in zip(pools, tokens):
            amount_in = await pool.get_amount_out_exact_in(amount_in, token_out, safety_margin)
        return amount_in

    async def should_reverse(self, amount_in: OsmosisTokenAmount) -> bool:
        assert self.is_cycle, "Reversion testing only applicable to cycles"
        amount_forward, amount_reverse = await asyncio.gather(
            self.get_amount_out_swap_exact_in(amount_in, reverse=False, safety_margin=False),
            self.get_amount_out_swap_exact_in(amount_in, reverse=True, safety_margin=False),
        )
        return amount_reverse > amount_forward
