from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from enum import Enum
from typing import TypeVar

from cosmos_sdk.core import AccAddress
from cosmos_sdk.core.wasm import MsgExecuteContract

from ..client import TerraClient
from ..terraswap.liquidity_pair import LiquidityPair as TerraswapLiquidityPair
from ..token import TerraTokenAmount

log = logging.getLogger(__name__)

FEE = Decimal("0.003")

AmountTuple = tuple[TerraTokenAmount, TerraTokenAmount]
_LiquidityPairT = TypeVar("_LiquidityPairT", bound="LiquidityPair")


class PairType(str, Enum):
    xyk = "xyk"
    stable = "stable"


class LiquidityPair(TerraswapLiquidityPair):
    pair_type: PairType

    @classmethod
    async def new(
        cls: type[_LiquidityPairT],
        contract_addr: AccAddress,
        client: TerraClient,
        fee_rate: Decimal = None,
        factory_name: str = None,
        factory_address: AccAddress = None,
        router_address: AccAddress = None,
        assert_limit_order_address: AccAddress = None,
        recursive_lp_token_code_id: int = None,
        check_liquidity: bool = True,
        fee_rates: dict[PairType, Decimal] = None,
    ) -> _LiquidityPairT:
        if contract_addr in cls.__instances or contract_addr in cls.__instances_creation:
            return await cls._get_instance(contract_addr, client, check_liquidity)
        cls.__instances_creation[contract_addr] = asyncio.Event()

        self = super().__new__(cls)

        fee_rates = fee_rates or {}
        fee_rate = fee_rate if fee_rate is not None else fee_rates.get(self.pair_type)
        self.pair_type = await self._get_pair_type()

        await self._init(
            contract_addr,
            client,
            fee_rate,
            factory_name,
            factory_address,
            router_address,
            assert_limit_order_address,
            recursive_lp_token_code_id,
            check_liquidity,
        )
        return self

    async def _get_pair_type(self) -> PairType:
        data = await self.client.contract_query(self.contract_addr, {"pair": {}})
        if PairType.xyk in data["pair_type"]:
            return PairType.xyk
        if PairType.stable in data["pair_type"]:
            return PairType.stable
        raise Exception(f"Could not parse pair_type={data['pair_type']}")

    async def get_swap_amounts(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
        simulate: bool = False,
        simulate_msg: MsgExecuteContract = None,
        max_spread: Decimal = None,
        belief_price: Decimal = None,
    ) -> dict[str, AmountTuple]:
        if self.pair_type == PairType.xyk:
            return await self._get_swap_amounts_xyk(
                amount_in, safety_margin, simulate, simulate_msg, max_spread, belief_price
            )
        if self.pair_type == PairType.stable:
            return await self._get_swap_amounts_stable(
                amount_in, safety_margin, simulate, simulate_msg, max_spread, belief_price
            )
        raise NotImplementedError

    async def _get_swap_amounts_xyk(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
        simulate: bool = False,
        simulate_msg: MsgExecuteContract = None,
        max_spread: Decimal = None,
        belief_price: Decimal = None,
    ) -> dict[str, AmountTuple]:
        if simulate:
            raise NotImplementedError(simulate_msg)
        raise NotImplementedError

    async def _get_swap_amounts_stable(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
        simulate: bool = False,
        simulate_msg: MsgExecuteContract = None,
        max_spread: Decimal = None,
        belief_price: Decimal = None,
    ) -> dict[str, AmountTuple]:
        if simulate:
            raise NotImplementedError(simulate_msg)
        raise NotImplementedError
