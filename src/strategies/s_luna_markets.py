from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from functools import partial
from typing import Any

from terra_sdk.core.auth import StdFee, TxInfo
from terra_sdk.core.wasm import MsgExecuteContract
from terra_sdk.exceptions import LCDResponseError

import utils
from arbitrage.terra import (
    TerraArbParams,
    TerraRepeatedTxArbitrage,
    TerraswapLPReserveSimulationMixin,
    run_strategy,
)
from chains.terra import LUNA, UST, NativeLiquidityPair, TerraClient, TerraTokenAmount, terraswap
from chains.terra.tx_filter import FilterSingleSwapTerraswapPair
from exceptions import TxError, UnprofitableArbitrage

from .common.default_params import (
    MAX_N_REPEATS,
    MAX_SINGLE_ARBITRAGE_AMOUNT,
    MIN_PROFIT_UST,
    MIN_UST_RESERVED_AMOUNT,
    OPTIMIZATION_TOLERANCE,
)

log = logging.getLogger(__name__)

MIN_START_AMOUNT = UST.to_amount(200)
ESTIMATED_GAS_USE = 475_000


class Direction(str, Enum):
    terraswap_first = "terraswap_first"
    native_first = "native_first"


@dataclass
class ArbParams(TerraArbParams):
    timestamp_found: float
    block_found: int

    prices: dict[str, Decimal]
    terra_virtual_pools: tuple[Decimal, Decimal]
    pool_reserves: tuple[TerraTokenAmount, TerraTokenAmount]
    ust_balance: Decimal
    direction: Direction

    initial_amount: TerraTokenAmount
    msgs: list[MsgExecuteContract]
    n_repeat: int
    est_final_amount: TerraTokenAmount
    est_fee: StdFee
    est_net_profit_usd: Decimal

    def to_data(self) -> dict:
        return {
            "timestamp_found": self.timestamp_found,
            "block_found": self.block_found,
            "prices": {key: float(price) for key, price in self.prices.items()},
            "terra_virtual_pools": [float(vp) for vp in self.terra_virtual_pools],
            "pool_reserves": [reserve.to_data() for reserve in self.pool_reserves],
            "direction": self.direction,
            "initial_amount": self.initial_amount.to_data(),
            "msgs": [msg.to_data() for msg in self.msgs],
            "n_repeat": self.n_repeat,
            "est_final_amount": self.est_final_amount.to_data(),
            "est_fee": self.est_fee.to_data(),
            "est_net_profit_usd": float(self.est_net_profit_usd),
        }


async def get_arbitrages(client: TerraClient) -> list[LunaUstMarketArbitrage]:
    factory = await terraswap.TerraswapFactory.new(client)

    terraswap_pair = await factory.get_pair("UST-LUNA")
    native_pair = NativeLiquidityPair(client, (UST, LUNA))
    router = factory.get_router([terraswap_pair, native_pair])

    return [LunaUstMarketArbitrage(client, router, terraswap_pair)]


def get_filters(
    arb_routes: list[LunaUstMarketArbitrage],
) -> dict[terraswap.HybridLiquidityPair, FilterSingleSwapTerraswapPair]:
    filters: dict[terraswap.HybridLiquidityPair, FilterSingleSwapTerraswapPair] = {}
    for arb_route in arb_routes:
        for pair in arb_route.pairs:
            if not isinstance(pair, terraswap.LiquidityPair):
                raise NotImplementedError
            filters[pair] = FilterSingleSwapTerraswapPair(pair)
    return filters


class LunaUstMarketArbitrage(TerraswapLPReserveSimulationMixin, TerraRepeatedTxArbitrage):
    def __init__(
        self,
        client: TerraClient,
        router: terraswap.Router,
        terraswap_pool: terraswap.LiquidityPair,
    ):
        self.router = router
        self.terraswap_pool = terraswap_pool
        self._route_native_first: list[terraswap.RouteStep] = [
            terraswap.RouteStepNative(UST, LUNA),
            terraswap.RouteStepTerraswap(LUNA, UST),
        ]
        self._route_terraswap_first: list[terraswap.RouteStep] = [
            terraswap.RouteStepTerraswap(UST, LUNA),
            terraswap.RouteStepNative(LUNA, UST),
        ]

        pairs = [self.terraswap_pool]
        super().__init__(client, pairs=pairs, filter_keys=pairs)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.terraswap_pool.repr_symbol})"

    def _reset_mempool_params(self):
        super()._reset_mempool_params()

    async def _get_arbitrage_params(
        self,
        height: int,
        filtered_mempool: dict[Any, list[list[dict]]] = None,
    ) -> ArbParams:
        async with self._simulate_reserve_changes(filtered_mempool):
            prices = await self._get_prices()
            terraswap_premium = prices["terraswap"] / prices["market"] - 1
            if terraswap_premium > 0:
                direction = Direction.native_first
                route = self._route_native_first
            else:
                direction = Direction.terraswap_first
                route = self._route_terraswap_first
            ust_balance = (await UST.get_balance(self.client)).amount

            initial_amount = await self._get_optimal_argitrage_amount(route, terraswap_premium)
            single_initial_amount, n_repeat = self._check_repeats(initial_amount, ust_balance)
            single_final_amount, msgs = await self.router.op_swap(
                self.client.address,
                single_initial_amount,
                route,
                min_amount_out=single_initial_amount,
                safety_margin=True,
            )
            final_amount = single_final_amount * n_repeat
            try:
                fee = await self.client.tx.estimate_fee(
                    msgs,
                    use_fallback_estimate=self._simulating_reserve_changes,
                    estimated_gas_use=ESTIMATED_GAS_USE,
                )
            except LCDResponseError as e:
                log.debug(
                    "Error when estimating fee",
                    extra={
                        "data": {
                            "terraswap_premium": f"{terraswap_premium:.3%}",
                            "direction": direction,
                            "msgs": [msg.to_data() for msg in msgs],
                        },
                    },
                    exc_info=True,
                )
                raise TxError(e)
        gas_cost = TerraTokenAmount.from_coin(*fee.amount) * n_repeat
        gas_cost_raw = gas_cost.amount / self.client.gas_adjustment
        net_profit_ust = (final_amount - initial_amount).amount - gas_cost_raw
        if net_profit_ust < MIN_PROFIT_UST:
            raise UnprofitableArbitrage(
                f"Low profitability: USD {net_profit_ust:.2f}, {terraswap_premium=:0.3%}"
            )

        return ArbParams(
            timestamp_found=time.time(),
            block_found=height,
            prices=prices,
            terra_virtual_pools=await self.client.market.get_virtual_pools(),
            ust_balance=ust_balance,
            pool_reserves=await self.terraswap_pool.get_reserves(),
            direction=direction,
            initial_amount=initial_amount,
            msgs=msgs,
            n_repeat=n_repeat,
            est_final_amount=final_amount,
            est_fee=fee,
            est_net_profit_usd=net_profit_ust,
        )

    async def _get_prices(self) -> dict[str, Decimal]:
        reserves = await self.terraswap_pool.get_reserves()
        terraswap_price = reserves[0].amount / reserves[1].amount
        market_price = await self.client.oracle.get_exchange_rate(LUNA, UST)
        return {
            "terraswap": terraswap_price,
            "market": market_price,
        }

    async def _get_optimal_argitrage_amount(
        self,
        route: list[terraswap.RouteStep],
        terraswap_premium: Decimal,
    ) -> TerraTokenAmount:
        profit = await self._get_gross_profit(MIN_START_AMOUNT, route)
        if profit < 0:
            raise UnprofitableArbitrage(f"No profitability, {terraswap_premium=:0.3%}")
        func = partial(self._get_gross_profit_dec, route=route)
        ust_amount, _ = await utils.aoptimization.optimize(
            func,
            x0=MIN_START_AMOUNT.amount,
            dx=MIN_START_AMOUNT.dx,
            tol=OPTIMIZATION_TOLERANCE.amount,
        )
        return UST.to_amount(ust_amount)

    async def _get_gross_profit(
        self,
        initial_lp_amount: TerraTokenAmount,
        route: list[terraswap.RouteStep],
        safety_round: bool = False,
    ) -> TerraTokenAmount:
        amount_out = await self.router.get_swap_amount_out(initial_lp_amount, route, safety_round)
        return amount_out - initial_lp_amount

    async def _get_gross_profit_dec(
        self,
        amount: Decimal,
        route: list[terraswap.RouteStep],
        safety_round: bool = False,
    ) -> Decimal:
        token_amount = UST.to_amount(amount)
        return (await self._get_gross_profit(token_amount, route, safety_round)).amount

    def _check_repeats(
        self,
        initial_amount: TerraTokenAmount,
        ust_balance: Decimal,
    ) -> tuple[TerraTokenAmount, int]:
        max_amount = min(ust_balance - MIN_UST_RESERVED_AMOUNT, MAX_SINGLE_ARBITRAGE_AMOUNT.amount)
        n_repeat = math.ceil(initial_amount.amount / max_amount)
        return initial_amount / n_repeat, min(n_repeat, MAX_N_REPEATS)

    async def _extract_returns_from_info(
        self,
        info: TxInfo,
    ) -> tuple[TerraTokenAmount, Decimal]:
        balance_changes = TerraClient.extract_coin_balance_changes(info.logs)
        arb_changes = balance_changes[self.client.address]
        assert all(change.token == UST for change in arb_changes)
        assert len(arb_changes) == 2
        return max(arb_changes), arb_changes[0].amount + arb_changes[1].amount


async def run(max_n_blocks: int = None):
    async with await TerraClient.new() as client:
        arb_routes = await get_arbitrages(client)
        mempool_filters = get_filters(arb_routes)
        await run_strategy(client, arb_routes, mempool_filters, max_n_blocks)
