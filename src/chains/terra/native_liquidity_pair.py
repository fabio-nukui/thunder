from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import TYPE_CHECKING, TypeVar

from terra_sdk.core import AccAddress

from .token import TerraNativeToken, TerraToken, TerraTokenAmount

if TYPE_CHECKING:
    from .client import TerraClient
    from .swap_utils import Operation

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
    async def op_swap(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = True,
    ) -> Operation:
        ...

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
        self.tokens = tokens if (tokens[0] < tokens[1]) else (tokens[1], tokens[0])
        self._stop_updates = False
        self._virtual_pool_changes = (Decimal(0), Decimal(0))

    def __hash__(self) -> int:
        return hash((self.__class__, self.tokens))

    def __eq__(self, other) -> bool:
        if isinstance(other, type(self)):
            return self.tokens == other.tokens
        return NotImplemented

    async def get_swap_amount_out(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
    ) -> TerraTokenAmount:
        assert amount_in.token in self.tokens
        token_out = self.tokens[0] if amount_in.token == self.tokens[1] else self.tokens[1]
        vp = await self.get_virtual_pools()
        return await self.client.market.get_amount_out(amount_in, token_out, safety_margin, vp)

    async def get_virtual_pools(self) -> tuple[Decimal, Decimal]:
        vp = await self.client.market.get_virtual_pools()
        return vp[0] + self._virtual_pool_changes[0], vp[1] + self._virtual_pool_changes[1]

    async def op_swap(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = True,
    ) -> Operation:
        raise NotImplementedError

    async def simulate_reserve_change(self, amounts: AmountTuple) -> NativeLiquidityPair:
        raise NotImplementedError

    async def get_reserve_changes_from_msg(self, msg: dict) -> AmountTuple:
        raise NotImplementedError
