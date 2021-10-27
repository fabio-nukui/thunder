from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TypeVar

from .client import TerraClient
from .token import TerraNativeToken, TerraToken, TerraTokenAmount

AmountTuple = tuple[TerraTokenAmount, TerraTokenAmount]
_BaseTerraLiquidityPairT = TypeVar("_BaseTerraLiquidityPairT", bound="BaseTerraLiquidityPair")


class BaseTerraLiquidityPair(ABC):
    client: TerraClient
    tokens: tuple[TerraToken, TerraToken]
    _stop_updates: bool

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.repr_symbol})"

    @property
    def repr_symbol(self) -> str:
        return f"{self.tokens[0].repr_symbol}/{self.tokens[1].repr_symbol}"

    @property
    def sorted_tokens(self) -> tuple[TerraToken, TerraToken]:
        if self.tokens[0] < self.tokens[1]:
            return self.tokens[0], self.tokens[1]
        return self.tokens[1], self.tokens[0]

    @abstractmethod
    async def get_swap_amount_out(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
    ) -> TerraTokenAmount:
        ...

    @abstractmethod
    async def simulate_reserve_change(
        self: _BaseTerraLiquidityPairT,
        amounts: AmountTuple,
    ) -> _BaseTerraLiquidityPairT:
        ...

    @abstractmethod
    async def get_reserve_changes_from_msg(self, msg: dict) -> AmountTuple:
        ...


class NativeLiquidityPair(BaseTerraLiquidityPair):
    tokens: tuple[TerraNativeToken, TerraNativeToken]

    def __init__(self, client: TerraClient, tokens: tuple[TerraNativeToken, TerraNativeToken]):
        self.client = client
        self.tokens = tokens

    async def get_swap_amount_out(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
    ) -> TerraTokenAmount:
        assert amount_in.token in self.tokens
        token_out = self.tokens[0] if amount_in.token == self.tokens[1] else self.tokens[1]
        return await self.client.market.get_amount_out(amount_in, token_out, safety_margin)

    async def simulate_reserve_change(self, amounts: AmountTuple) -> NativeLiquidityPair:
        if amounts[0].amount == amounts[1].amount == 0:
            return self
        raise NotImplementedError

    async def get_reserve_changes_from_msg(self, msg: dict) -> AmountTuple:
        raise NotImplementedError
