from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Sequence

from terra_sdk.core import AccAddress
from terra_sdk.core.wasm import MsgExecuteContract

from ..client import TerraClient
from ..token import CW20Token, TerraNativeToken, TerraToken, TerraTokenAmount
from .liquidity_pair import LiquidityPair
from .utils import Operation, token_to_data


class RouteStep(ABC):
    def __init__(
        self,
        token_in: TerraToken,
        token_out: TerraToken,
    ) -> None:
        self.token_in = token_in
        self.token_out = token_out

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(token_in={self.token_in}, token_out={self.token_out})"

    @property
    def sorted_tokens(self) -> tuple[TerraToken, TerraToken]:
        if self.token_in < self.token_out:
            return self.token_in, self.token_out
        return self.token_out, self.token_in

    @abstractmethod
    def to_data(self) -> dict:
        ...


class RouteStepTerraswap(RouteStep):
    def to_data(self) -> dict:
        return {
            "terra_swap": {
                "offer_asset_info": token_to_data(self.token_in),
                "ask_asset_info": token_to_data(self.token_out),
            }
        }


class RouteStepNative(RouteStep):
    def __init__(
        self,
        token_in: TerraNativeToken,
        token_out: TerraNativeToken,
    ):
        self.token_in = token_in
        self.token_out = token_out

    def to_data(self) -> dict:
        return {
            "native_swap": {
                "offer_denom": self.token_in.denom,
                "ask_denom": self.token_out.denom,
            }
        }


class Router:
    def __init__(
        self,
        contract_addr: AccAddress,
        liquidity_pairs: Iterable[LiquidityPair],
        client: TerraClient,
    ):
        self.pairs = {pair.sorted_tokens: pair for pair in liquidity_pairs}
        self.contract_addr = contract_addr
        self.client = client

    async def op_swap(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        route: Sequence[RouteStep],
        min_amount_out: TerraTokenAmount,
        safety_margin: bool | int = True,
    ) -> Operation:
        assert route, "route cannot be empty"

        swap_operations: list[dict] = []
        next_amount_in = await self.client.treasury.deduct_tax(amount_in)
        for step in route:
            if isinstance(step, RouteStepTerraswap):
                if step.sorted_tokens not in self.pairs:
                    raise Exception(f"No liquidity pair found for {step.sorted_tokens}")
                pair = self.pairs[step.sorted_tokens]
                next_amount_in = await pair.get_swap_amount_out(next_amount_in, safety_margin)
            else:
                assert isinstance(step.token_out, TerraNativeToken)
                next_amount_in = await self.client.market.get_amount_out(
                    next_amount_in, step.token_out, safety_margin
                )
            swap_operations.append(step.to_data())
        amount_out: TerraTokenAmount = next_amount_in

        swap_msg = {
            "execute_swap_operations": {
                "offer_amount": str(amount_in.int_amount),
                "minimum_receive": str(min_amount_out.int_amount),
                "operations": swap_operations,
            }
        }
        if isinstance(amount_in.token, CW20Token):
            contract = amount_in.token.contract_addr
            execute_msg = {
                "send": {
                    "contract": self.contract_addr,
                    "amount": str(amount_in.int_amount),
                    "msg": self.client.encode_msg(swap_msg),
                }
            }
            coins = []
        else:
            contract = self.contract_addr
            execute_msg = swap_msg
            coins = [amount_in.to_coin()]
        msg = MsgExecuteContract(
            sender=sender,
            contract=contract,
            execute_msg=execute_msg,
            coins=coins,
        )
        return amount_out, [msg]
