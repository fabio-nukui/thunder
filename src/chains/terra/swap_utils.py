from __future__ import annotations

import asyncio
import itertools
from functools import cache
from typing import Iterable, Sequence, Union, cast

from terra_sdk.core.strings import AccAddress
from terra_sdk.core.wasm import MsgExecuteContract

from .client import TerraClient
from .native_liquidity_pair import BaseTerraLiquidityPair, NativeLiquidityPair
from .terraswap.liquidity_pair import LiquidityPair
from .terraswap.router import Router, RouteStep, RouteStepNative, RouteStepTerraswap
from .token import TerraToken, TerraTokenAmount

Operation = tuple[TerraTokenAmount, list[MsgExecuteContract]]


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
        router_address: AccAddress = None,
    ):
        self.client = client
        self.list_steps = list_steps
        self.router_address = router_address
        self.pairs = [pair for step in list_steps for pair in step]
        self.tokens = _extract_tokens_from_routes(start_token, list_steps)
        self.n_steps = len(list_steps)

        self.is_cycle = self.tokens[0] == self.tokens[-1]
        self.routes = [
            SingleRoute(client, self.tokens, pairs, router_address)
            for pairs in itertools.product(*list_steps)
        ]
        self.n_routes = len(self.routes)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.repr_symbols}, n_routes={self.n_routes})"

    @property
    def repr_symbols(self) -> str:
        return _repr_route_symbols(self.tokens)


class SingleRoute:
    def __init__(
        self,
        client: TerraClient,
        tokens: Iterable[TerraToken],
        pairs: Iterable[BaseTerraLiquidityPair],
        router_address: AccAddress = None,
    ):
        self.client = client
        self.tokens = list(tokens)
        self.pairs = list(pairs)
        if router_address is None:
            self.router = None
        else:
            assert all(isinstance(p, (LiquidityPair, NativeLiquidityPair)) for p in pairs)
            pairs_r = cast(Iterable[Union[LiquidityPair, NativeLiquidityPair]], pairs)
            self.router = Router(router_address, pairs_r, client)
        self.is_cycle = self.tokens[0] == self.tokens[-1]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({_repr_route_symbols(self.tokens)})"

    async def should_reverse(self, amount_in: TerraTokenAmount) -> bool:
        assert self.is_cycle, "Reversion testing only applicable to cycles"
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
    ) -> Operation:
        if self.router is not None:
            min_amount_out = self._ensure_min_amount_out(amount_in, min_amount_out)
            route = self._get_route_steps(reverse)
            return await self.router.op_swap(
                self.client.address, amount_in, route, min_amount_out, safety_margin
            )
        pairs = self.pairs if not reverse else reversed(self.pairs)
        step_amount = amount_in
        msgs: list[MsgExecuteContract] = []
        for pair in pairs:
            if not isinstance(pair, LiquidityPair):
                raise NotImplementedError(
                    "SingleRoute.op_swap() only implemented for Router swaps"
                )
            step_amount, step_msgs = await pair.op_swap(
                self.client.address, step_amount, safety_margin
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

    @cache
    def _get_route_steps(self, reverse: bool) -> Sequence[RouteStep]:
        pairs, token_in = (
            (self.pairs, self.tokens[0])
            if not reverse
            else (reversed(self.pairs), self.tokens[-1])
        )
        steps: list[RouteStepNative | RouteStepTerraswap] = []
        for pair in pairs:
            token_out = pair.tokens[0] if token_in == pair.tokens[1] else pair.tokens[1]
            if isinstance(pair, LiquidityPair):
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
        pairs = self.pairs if not reverse else reversed(self.pairs)
        step_amount = amount_in
        if self.router is not None:
            route = self._get_route_steps(reverse)
            return await self.router.get_swap_amount_out(amount_in, route, safety_margin)
        for pair in pairs:
            step_amount = await pair.get_swap_amount_out(step_amount, safety_margin)
        return step_amount
