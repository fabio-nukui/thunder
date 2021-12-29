from __future__ import annotations

import asyncio
import base64
import json
import logging
from decimal import Decimal
from enum import Enum
from typing import TypeVar

from cosmos_sdk.core import AccAddress
from cosmos_sdk.core.wasm import MsgExecuteContract

from utils.cache import CacheGroup, ttl_cache

from ..client import TerraClient
from ..terraswap.liquidity_pair import LiquidityPair as TerraswapLiquidityPair
from ..token import TerraTokenAmount

log = logging.getLogger(__name__)

FEE = Decimal("0.003")
ROUTER_SWAP_ACTION = "astro_swap"

_AMP_CACHE_SIZE = 200
_ITERATIONS = 32
_N_COINS_STABLE = 2
_N_COINS_SQUARED = 4
_TOL = Decimal("0.000001")

AmountTuple = tuple[TerraTokenAmount, TerraTokenAmount]
_LiquidityPairT = TypeVar("_LiquidityPairT", bound="LiquidityPair")


class PairType(str, Enum):
    xyk = "xyk"
    stable = "stable"


async def _get_pair_type(client: TerraClient, contract_addr: AccAddress) -> PairType:
    data = await client.contract_query(contract_addr, {"pair": {}})
    if PairType.xyk in data["pair_type"]:
        return PairType.xyk
    if PairType.stable in data["pair_type"]:
        return PairType.stable
    raise Exception(f"Could not parse pair_type={data['pair_type']}")


def _compute_d(leverage: Decimal, amount_a: Decimal, amount_b: Decimal) -> Decimal:
    amount_a_times_coins = amount_a * _N_COINS_STABLE
    amount_b_times_coins = amount_b * _N_COINS_STABLE
    sum_x = amount_a + amount_b
    if not sum_x:
        return Decimal()

    d = sum_x
    for _ in range(_ITERATIONS):
        d_product = d ** 3 / (amount_a_times_coins * amount_b_times_coins)
        d_previous = d
        d = (
            (leverage * sum_x + d_product * _N_COINS_STABLE)
            * d_previous
            / ((leverage - 1) * d_previous + (_N_COINS_STABLE + 1) * d_product)
        )
        if abs(d - d_previous) <= _TOL:
            break
    return d


def _compute_new_reserve_out(leverage: Decimal, new_reserve_in: Decimal, d: Decimal) -> Decimal:
    c = d ** (_N_COINS_STABLE + 1) / (new_reserve_in * _N_COINS_SQUARED * leverage)
    b = new_reserve_in + d / leverage

    y = d
    for _ in range(_ITERATIONS):
        y_prev = y
        y = (y ** 2 + c) / (y * 2 + b - d)
        if abs(y - y_prev) <= _TOL:
            break
    return y


class LiquidityPair(TerraswapLiquidityPair):
    pair_type: PairType
    router_swap_acton = ROUTER_SWAP_ACTION

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

        self.pair_type = await _get_pair_type(client, contract_addr)
        fee_rates = fee_rates or {}
        fee_rate = fee_rate if fee_rate is not None else fee_rates.get(self.pair_type)

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

    @ttl_cache(CacheGroup.TERRA, _AMP_CACHE_SIZE)
    async def _get_amp(self) -> Decimal:
        if self.pair_type != PairType.stable:
            raise TypeError("amp only valid for stable pairs")
        config = await self.client.contract_query(self.contract_addr, {"config": {}})
        params = json.loads(base64.b64decode(config["params"]))
        return Decimal(params["amp"])

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
            return await super().get_swap_amounts(
                amount_in, safety_margin, simulate, simulate_msg, max_spread, belief_price
            )
        if self.pair_type == PairType.stable:
            return await self._get_swap_amounts_stable(
                amount_in, safety_margin, simulate, simulate_msg, max_spread, belief_price
            )
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
        """Based on
        https://github.com/astroport-fi/astroport-core/blob/v1.0.0/contracts/pair_stable/src/contract.rs#L1165  # noqa: E501
        """
        if simulate:
            raise NotImplementedError(simulate_msg)

        reserve_in, reserve_out = await self._get_in_out_reserves(amount_in=amount_in)
        amp = await self._get_amp()

        leverage = amp * _N_COINS_STABLE
        new_reserve_in = reserve_in + amount_in

        d = _compute_d(leverage, reserve_in.amount, reserve_out.amount)
        new_reserve_out = _compute_new_reserve_out(leverage, new_reserve_in.amount, d)
        amount_out_before_fees = (reserve_out - new_reserve_out).safe_margin(safety_margin)

        self._assert_max_spread(
            amount_in.amount,
            amount_out_before_fees.amount,
            reserve_in.amount,
            reserve_out.amount,
            max_spread,
            belief_price,
        )

        fee = amount_out_before_fees * self.fee_rate
        amount_out_before_taxes = amount_out_before_fees - fee

        tax = await self.client.treasury.calculate_tax(amount_out_before_taxes)
        amount_out = amount_out_before_taxes - tax

        return {
            "amounts_out": (amount_in * 0, amount_out),
            "fees": (amount_in * 0, fee),
            "taxes": (amount_in * 0, tax),
            "pool_change": (amount_in, -amount_out - tax),
        }
