from __future__ import annotations

from abc import ABC, abstractmethod
from functools import reduce
from typing import TypeVar

from terra_sdk.core import AccAddress
from terra_sdk.core.tx import Tx
from terra_sdk.core.wasm import MsgExecuteContract

from exceptions import MaxSpreadAssertion

from .client import OsmosisClient
from .token import OsmosisNativeToken, OsmosisToken, OsmosisTokenAmount

AmountTuple = tuple[OsmosisTokenAmount, OsmosisTokenAmount]
Operation = tuple[OsmosisTokenAmount, list[MsgExecuteContract]]

_BaseOsmoLiquidityPairT = TypeVar("_BaseOsmoLiquidityPairT", bound="BaseOsmosisLiquidityPair")


class BaseOsmosisLiquidityPair(ABC):
    client: OsmosisClient
    tokens: tuple[OsmosisToken, OsmosisToken]
    _stop_updates: bool

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.repr_symbol})"

    @property
    def repr_symbol(self) -> str:
        return f"{self.tokens[0].repr_symbol}/{self.tokens[1].repr_symbol}"

    @property
    def sorted_tokens(self) -> tuple[OsmosisToken, OsmosisToken]:
        if self.tokens[0] < self.tokens[1]:
            return self.tokens[0], self.tokens[1]
        return self.tokens[1], self.tokens[0]

    @abstractmethod
    async def op_swap(
        self,
        sender: AccAddress,
        amount_in: OsmosisTokenAmount,
        safety_margin: bool | int = True,
    ) -> Operation:
        ...

    @abstractmethod
    async def get_swap_amount_out(
        self,
        amount_in: OsmosisTokenAmount,
        safety_margin: bool | int = False,
    ) -> OsmosisTokenAmount:
        ...

    @abstractmethod
    async def simulate_reserve_change(
        self: _BaseOsmoLiquidityPairT,
        amounts: AmountTuple,
    ) -> _BaseOsmoLiquidityPairT:
        ...

    @abstractmethod
    async def get_reserve_changes_from_msg(self, msg: dict) -> AmountTuple:
        ...

    async def get_reserve_changes_from_tx(self, tx: Tx) -> AmountTuple:
        changes: list[AmountTuple] = []
        errors = []
        for msg in tx.body.messages:
            try:
                change = await self.get_reserve_changes_from_msg(msg.to_data())
                changes.append(self.fix_amounts_order(change))
            except MaxSpreadAssertion:
                raise
            except Exception as e:
                if len(tx.body.messages) == 1:
                    raise e
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


class GAMMLiquidityPair(BaseOsmosisLiquidityPair):
    tokens: tuple[OsmosisNativeToken, OsmosisNativeToken]

    def __init__(self, client: OsmosisClient, address: AccAddress):
        raise NotImplementedError

    async def op_swap(
        self,
        sender: AccAddress,
        amount_in: OsmosisTokenAmount,
        safety_margin: bool | int = True,
    ) -> Operation:
        ...

    async def get_swap_amount_out(
        self,
        amount_in: OsmosisTokenAmount,
        safety_margin: bool | int = False,
    ) -> OsmosisTokenAmount:
        ...

    async def simulate_reserve_change(
        self: _BaseOsmoLiquidityPairT,
        amounts: AmountTuple,
    ) -> _BaseOsmoLiquidityPairT:
        ...

    async def get_reserve_changes_from_msg(self, msg: dict) -> AmountTuple:
        ...
