from __future__ import annotations

from abc import ABC, abstractmethod
from copy import copy
from decimal import Decimal
from functools import reduce
from typing import TYPE_CHECKING, TypeVar

from terra_sdk.core import AccAddress

from exceptions import MaxSpreadAssertion

from .denoms import LUNA, SDT
from .token import TerraNativeToken, TerraToken, TerraTokenAmount

if TYPE_CHECKING:
    from .client import TerraClient
    from .swap_utils import Operation

AmountTuple = tuple[TerraTokenAmount, TerraTokenAmount]
_BaseTerraLiquidityPairT = TypeVar("_BaseTerraLiquidityPairT", bound="BaseTerraLiquidityPair")
_NativeLiquidityPairT = TypeVar("_NativeLiquidityPairT", bound="NativeLiquidityPair")


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

    async def get_reserve_changes_from_msgs(self, msgs: list[dict]) -> AmountTuple:
        changes: list[AmountTuple] = []
        errors = []
        for msg in msgs:
            try:
                change = await self.get_reserve_changes_from_msg(msg["value"])
                changes.append(self.fix_amounts_order(change))
            except MaxSpreadAssertion:
                raise
            except Exception as e:
                errors.append(e)
        if not changes:
            raise Exception(f"Error when parsing msgs: {errors}")
        return reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]), changes)

    def fix_amounts_order(self, amounts: AmountTuple) -> AmountTuple:
        if (amounts[1].token, amounts[0].token) == self.tokens:
            return amounts[1], amounts[0]
        if (amounts[0].token, amounts[1].token) == self.tokens:
            return amounts
        raise Exception("Tokens in amounts do not match reserves")


class NativeLiquidityPair(BaseTerraLiquidityPair):
    tokens: tuple[TerraNativeToken, TerraNativeToken]

    def __init__(self, client: TerraClient, tokens: tuple[TerraNativeToken, TerraNativeToken]):
        self.client = client
        self.tokens = tokens if (tokens[0] < tokens[1]) else (tokens[1], tokens[0])
        self._stop_updates = False
        self._pool_delta_changes = Decimal(0)

    def __hash__(self) -> int:
        return hash((self.__class__, self.tokens))

    def __eq__(self, other) -> bool:
        if isinstance(other, type(self)):
            return self.tokens == other.tokens
        return NotImplemented

    async def get_swap_amounts(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
    ) -> dict[str, AmountTuple]:
        amount_out = await self.get_swap_amount_out(amount_in, safety_margin)
        return {
            "pool_change": (amount_in, -amount_out),
            "amounts_out": (amount_in * 0, amount_out),
        }

    async def get_swap_amount_out(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
    ) -> TerraTokenAmount:
        assert amount_in.token in self.tokens
        token_out = self.tokens[0] if amount_in.token == self.tokens[1] else self.tokens[1]
        return await self.client.market.get_amount_out(
            amount_in, token_out, safety_margin, self._pool_delta_changes
        )

    async def op_swap(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = True,
    ) -> Operation:
        raise NotImplementedError

    async def simulate_reserve_change(
        self: _NativeLiquidityPairT,
        amounts: AmountTuple,
    ) -> _NativeLiquidityPairT:
        """Based on https://github.com/terra-money/core/blob/v0.5.10/x/market/keeper/swap.go#L15"""
        assert isinstance(amounts[0].token, TerraNativeToken)
        assert isinstance(amounts[1].token, TerraNativeToken)
        simulation = copy(self)

        if LUNA not in (amounts[0].token, amounts[1].token):
            return simulation

        change_terra = amounts[0] if amounts[1].token == LUNA else amounts[1]
        change_sdt = await self.client.market.compute_swap_no_spread(change_terra, SDT)
        simulation._pool_delta_changes += change_sdt.amount
        return simulation

    async def get_reserve_changes_from_msg(self, msg: dict) -> AmountTuple:
        token = TerraNativeToken(msg["offer_coin"]["denom"])
        assert token in self.tokens
        assert msg["ask_denom"] in (self.tokens[0].denom, self.tokens[1].denom)

        amount_in = token.to_amount(int_amount=msg["offer_coin"]["amount"])
        amounts = await self.get_swap_amounts(amount_in)

        return amounts["pool_change"]
