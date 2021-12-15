from __future__ import annotations

import asyncio
import itertools
from typing import Iterable, Sequence, Union, cast

from cosmos_sdk.core import AccAddress
from cosmos_sdk.core.msg import Msg

from utils.cache import lru_cache

from .client import TerraClient
from .native_liquidity_pair import BaseTerraLiquidityPair
from .terraswap.liquidity_pair import LiquidityPair, RouterNativeLiquidityPair
from .terraswap.router import Router, RouteStep, RouteStepNative, RouteStepTerraswap
from .token import TerraToken, TerraTokenAmount

Operation = tuple[TerraTokenAmount, Sequence[Msg]]


def _extract_tokens_from_routes(
    start_token: TerraToken, list_routes: Sequence[Sequence[BaseTerraLiquidityPair]]
) -> tuple[TerraToken, ...]:
    token_from = start_token
    tokens = [token_from]
    for step in list_routes:
        step_tokens = step[0].tokens
        token_to = step_tokens[1] if token_from == step_tokens[0] else step_tokens[0]
        if not all(token_from in pair.tokens and token_to in pair.tokens for pair in step):
            raise Exception(f"Pair with wrong tokens(s) {step=}, {token_from=}, {token_to=}")
        tokens.append(token_to)
        token_from = token_to
    return tuple(tokens)


def _repr_route_symbols(tokens: Iterable[TerraToken]):
    return f"{'->'.join(token.symbol for token in tokens)}"


class MultiRoutes:
    def __init__(
        self,
        client: TerraClient,
        start_token: TerraToken,
        list_steps: Sequence[Sequence[BaseTerraLiquidityPair]],
        single_direction: bool = False,
        router_address: AccAddress = None,
    ):
        self.client = client
        self.list_steps = list_steps
        self.router_address = router_address
        self.pools = [pool for step in list_steps for pool in step]
        self.tokens = _extract_tokens_from_routes(start_token, list_steps)
        self.n_steps = len(list_steps)

        self.is_cycle = self.tokens[0] == self.tokens[-1]
        self.routes = [
            RoutePools(client, self.tokens, pools, single_direction, router_address)
            for pools in itertools.product(*list_steps)
        ]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.repr_symbols}, n_routes={self.n_routes})"

    @property
    def repr_symbols(self) -> str:
        return _repr_route_symbols(self.tokens)

    @property
    def n_routes(self) -> int:
        return len(self.routes)


class RoutePools:
    def __init__(
        self,
        client: TerraClient,
        tokens: Iterable[TerraToken],
        pools: Iterable[BaseTerraLiquidityPair],
        single_direction: bool = False,
        router_address: AccAddress = None,
    ):
        self.client = client
        self.tokens = list(tokens)
        self._pools = list(pools)
        self.single_direction = single_direction
        if router_address is None:
            self.router = None
        else:
            assert all(isinstance(p, (LiquidityPair, RouterNativeLiquidityPair)) for p in pools)
            pools_r = cast(Iterable[Union[LiquidityPair, RouterNativeLiquidityPair]], pools)
            self.router = Router(router_address, pools_r, client)
        self.is_cycle = self.tokens[0] == self.tokens[-1]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({_repr_route_symbols(self.tokens)})"

    @property
    def pools(self) -> list[BaseTerraLiquidityPair]:
        return self._pools

    @pools.setter
    def pools(self, pools: list[BaseTerraLiquidityPair]):
        self._pools = pools
        if self.router is not None:
            for pool in pools:
                if isinstance(pool, LiquidityPair):
                    self.router.terraswap_pairs[pool.sorted_tokens] = pool
                elif isinstance(pool, RouterNativeLiquidityPair):
                    self.router.native_pairs[pool.sorted_tokens] = pool

    async def should_reverse(self, amount_in: TerraTokenAmount) -> bool:
        assert self.is_cycle, "Reversion testing only applicable to cycles"
        if self.single_direction:
            return False
        amount_forward, amount_reverse = await asyncio.gather(
            self.get_swap_amount_out(amount_in, reverse=False, safety_margin=False),
            self.get_swap_amount_out(amount_in, reverse=True, safety_margin=False),
        )
        return amount_reverse > amount_forward

    async def op_swap(
        self,
        amount_in: TerraTokenAmount,
        reverse: bool = False,
        safety_margin: bool | int = True,
        min_amount_out: TerraTokenAmount = None,
        simulate: bool = False,
    ) -> Operation:
        min_amount_out = self._ensure_min_amount_out(amount_in, min_amount_out)
        if self.router is not None:
            route = self._get_route_steps(reverse)
            return await self.router.op_swap(
                self.client.address, amount_in, route, min_amount_out, safety_margin, simulate
            )
        pools = self.pools if not reverse else self.pools[::-1]
        step_amount = amount_in
        msgs: list[Msg] = []
        for pool in pools:
            if isinstance(pool, RouterNativeLiquidityPair) and pool == pools[-1]:
                step_amount, step_msgs = await pool.op_swap(
                    self.client.address, step_amount, safety_margin, simulate, min_amount_out
                )
            else:
                step_amount, step_msgs = await pool.op_swap(
                    self.client.address, step_amount, safety_margin, simulate
                )
            msgs.extend(step_msgs)
        return step_amount, msgs

    def _ensure_min_amount_out(
        self,
        amount_in: TerraTokenAmount,
        min_amount_out: TerraTokenAmount | None,
    ) -> TerraTokenAmount:
        if min_amount_out is None:
            if self.is_cycle:
                return amount_in
            else:
                raise TypeError("Missing min_amount_out")
        return min_amount_out

    @lru_cache()
    def _get_route_steps(self, reverse: bool) -> Sequence[RouteStep]:
        pools, token_in = (
            (self.pools, self.tokens[0])
            if not reverse
            else (reversed(self.pools), self.tokens[-1])
        )
        steps: list[RouteStepNative | RouteStepTerraswap] = []
        for pool in pools:
            token_out = pool.tokens[0] if token_in == pool.tokens[1] else pool.tokens[1]
            if isinstance(pool, LiquidityPair):
                steps.append(RouteStepTerraswap(token_in, token_out))
            else:
                steps.append(RouteStepNative(token_in, token_out))  # type: ignore
            token_in = token_out
        return steps

    async def get_swap_amount_out(
        self,
        amount_in: TerraTokenAmount,
        reverse: bool = False,
        safety_margin: bool | int = True,
    ) -> TerraTokenAmount:
        if self.router is not None:
            route = self._get_route_steps(reverse)
            return await self.router.get_swap_amount_out(amount_in, route, safety_margin)
        pools = self.pools if not reverse else reversed(self.pools)
        step_amount = amount_in
        for pool in pools:
            step_amount = await pool.get_swap_amount_out(step_amount, safety_margin)
        return step_amount
