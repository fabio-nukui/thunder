from __future__ import annotations

import asyncio
import itertools
from decimal import Decimal
from typing import Iterable

from terra_sdk.core.wasm.msgs import MsgExecuteContract

from common.token import Token

from ..client import TerraClient
from ..token import TerraToken, TerraTokenAmount
from .liquidity_pair import LiquidityPair
from .utils import Operation


def _extract_tokens_from_routes(
    start_token: TerraToken,
    list_routes: list[list[LiquidityPair]],
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


def _repr_route_symbols(tokens: Iterable[Token]):
    return f"({'->'.join(token.symbol for token in tokens)})"


class MultiRoutes:
    def __init__(
        self,
        client: TerraClient,
        start_token: TerraToken,
        list_steps: list[list[LiquidityPair]],
    ):
        self.client = client
        self.list_steps = list_steps
        self.pairs = [pair for step in list_steps for pair in step]
        self.tokens = _extract_tokens_from_routes(start_token, list_steps)

        self.is_cycle = self.tokens[0] == self.tokens[-1]
        self.routes = [
            SingleRoute(client, self.tokens, pairs) for pairs in itertools.product(*list_steps)
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
        pairs: Iterable[LiquidityPair],
    ):
        self.client = client
        self.tokens = list(tokens)
        self.pairs = list(pairs)
        self.is_cycle = self.tokens[0] == self.tokens[-1]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({_repr_route_symbols(self.tokens)})"

    async def should_reverse(self, amount_in: TerraTokenAmount) -> bool:
        assert self.is_cycle, "Reversion testing only applicable to cycles"
        (amount_forward, _), (amount_reverse, _) = await asyncio.gather(
            self.op_swap(amount_in, reverse=False, safety_margin=False),
            self.op_swap(amount_in, reverse=True, safety_margin=False),
        )
        return amount_reverse > amount_forward

    async def op_swap(
        self,
        amount_in: TerraTokenAmount,
        reverse: bool = False,
        max_slippage: Decimal = None,
        safety_margin: bool | int = True,
    ) -> Operation:
        pairs = self.pairs if not reverse else reversed(self.pairs)
        step_amount = amount_in
        msgs: list[MsgExecuteContract] = []
        for pair in pairs:
            step_amount, step_msgs = await pair.op_swap(
                self.client.address, step_amount, max_slippage, safety_margin
            )
            msgs.extend(step_msgs)
        return step_amount, msgs

    async def get_swap_amount_out(
        self,
        amount_in: TerraTokenAmount,
        reverse: bool = False,
        safety_margin: bool | int = True,
    ) -> TerraTokenAmount:
        pairs = self.pairs if not reverse else reversed(self.pairs)
        step_amount = amount_in
        for pair in pairs:
            step_amount = await pair.get_swap_amount_out(step_amount, safety_margin)
        return step_amount
