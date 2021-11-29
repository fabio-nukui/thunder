from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass
from decimal import Decimal
from functools import partial
from typing import Any, Sequence

from cosmos_sdk.core.auth import TxInfo
from cosmos_sdk.core.fee import Fee
from cosmos_sdk.core.tx import Tx
from cosmos_sdk.core.wasm import MsgExecuteContract
from cosmos_sdk.exceptions import LCDResponseError

import utils
from arbitrage.cosmos import (
    CosmosArbParams,
    CosmosRepeatedTxArbitrage,
    LPReserveSimulationMixin,
    run_strategy,
)
from chains.cosmos import ibc_denoms
from chains.cosmos.osmosis import (
    GAMMLiquidityPool,
    OsmosisClient,
    OsmosisNativeToken,
    OsmosisTokenAmount,
)
from chains.cosmos.osmosis.route import MultiRoutes, RoutePools
from chains.cosmos.osmosis.tx_filter import FilterSwap
from exceptions import FeeEstimationError, UnprofitableArbitrage
from strategies.common.default_params import MAX_N_REPEATS
from utils.cache import CacheGroup

log = logging.getLogger(__name__)

MIN_START_AMOUNT_UST = Decimal(10)
MAX_SINGLE_ARBITRAGE_AMOUNT_UST = Decimal(50_000)
OPTIMIZATION_TOLERANCE_UST = Decimal("0.05")
MIN_PROFIT_UST = Decimal("0.1")


class NoPairFound(Exception):
    pass


@dataclass
class ArbParams(CosmosArbParams):
    __slots__ = (
        "timestamp_found",
        "block_found",
        "initial_balance",
        "route",
        "reverse",
        "initial_amount",
        "msgs",
        "n_repeat",
        "est_final_amount",
        "est_fee",
        "est_net_profit_usd",
    )
    timestamp_found: float
    block_found: int

    initial_balance: OsmosisTokenAmount
    route: RoutePools
    reverse: bool

    initial_amount: OsmosisTokenAmount
    msgs: list[MsgExecuteContract]
    n_repeat: int
    est_final_amount: OsmosisTokenAmount
    est_fee: Fee
    est_net_profit_usd: Decimal

    def to_data(self) -> dict:
        return {
            "timestamp_found": self.timestamp_found,
            "block_found": self.block_found,
            "initial_balance": self.initial_balance.to_data(),
            "route": str(self.route),
            "reverse": self.reverse,
            "initial_amount": self.initial_amount.to_data(),
            "msgs": [msg.to_data() for msg in self.msgs],
            "n_repeat": self.n_repeat,
            "est_final_amount": self.est_final_amount.to_data(),
            "est_fee": self.est_fee.to_data(),
            "est_net_profit_usd": float(self.est_net_profit_usd),
        }


async def get_arbitrages(client: OsmosisClient) -> list[OsmosisCyclesArbitrage]:
    raise NotImplementedError


def get_filters(
    arb_routes: list[OsmosisCyclesArbitrage],
) -> dict[GAMMLiquidityPool, FilterSwap]:
    return {pool: FilterSwap(pool) for arb_route in arb_routes for pool in arb_route.pools}


class OsmosisCyclesArbitrage(
    LPReserveSimulationMixin, CosmosRepeatedTxArbitrage[OsmosisClient]
):
    routes: Sequence[RoutePools]
    start_token: OsmosisNativeToken
    gas_adjustment: Decimal | None
    UST: OsmosisNativeToken
    estimated_gas_use: int
    min_start_amount: OsmosisTokenAmount
    max_single_arbitrage: OsmosisTokenAmount
    optimization_tolerance: OsmosisTokenAmount
    pools: list[GAMMLiquidityPool]

    @classmethod
    async def new(
        cls,
        client: OsmosisClient,
        multi_routes: MultiRoutes,
        gas_adjustment: Decimal = None,
    ) -> OsmosisCyclesArbitrage:
        self = super().__new__(cls)
        assert multi_routes.is_cycle

        self.start_token = multi_routes.start_token
        self.gas_adjustment = gas_adjustment

        self.UST = OsmosisNativeToken(ibc_denoms.get_ibc_denom("UST", client.chain_id), client)
        min_start_amount = self.UST.to_amount(MIN_START_AMOUNT_UST)
        max_single_arbitrage_amount = self.UST.to_amount(MAX_SINGLE_ARBITRAGE_AMOUNT_UST)
        optimization_tolerance = self.UST.to_amount(OPTIMIZATION_TOLERANCE_UST)

        (
            self.min_start_amount,
            self.max_single_arbitrage,
            self.optimization_tolerance,
        ) = await asyncio.gather(
            client.gamm.get_best_amount_out(min_start_amount, self.start_token),
            client.gamm.get_best_amount_out(max_single_arbitrage_amount, self.start_token),
            client.gamm.get_best_amount_out(optimization_tolerance, self.start_token),
        )

        self.__init__(
            client,
            pools=multi_routes.pools,
            routes=multi_routes.routes,
            filter_keys=multi_routes.pools,
            fee_denom=self.start_token.denom,
        )
        self.estimated_gas_use = await self._estimate_gas_use()
        return self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(start_token={self.start_token}, pools={self.pools})"

    def _reset_mempool_params(self):
        super()._reset_mempool_params()

    async def _estimate_gas_use(self) -> int:
        longest_route = max(self.routes, key=lambda x: len(x.pools))
        try:
            _, msgs = await longest_route.op_swap_exact_in(
                self.min_start_amount, min_amount_out=self.start_token.to_amount(0)
            )
            fee = await self.client.tx.estimate_fee(msgs)
        except Exception as e:
            raise FeeEstimationError(e)
        return fee.gas_limit

    async def _get_arbitrage_params(
        self,
        height: int,
        filtered_mempool: dict[Any, list[Tx]] = None,
    ) -> CosmosArbParams:
        initial_balance = await self.start_token.get_balance(self.client)

        params: list[dict] = []
        errors: list[Exception] = []
        async with self._simulate_reserve_changes(filtered_mempool):
            for route in self.routes:
                try:
                    params.append(await self._get_params_single_route(route, initial_balance))
                except (FeeEstimationError, UnprofitableArbitrage) as e:
                    errors.append(e)
        if not params:
            raise UnprofitableArbitrage(errors)
        best_param = max(params, key=lambda x: x["net_profit"])
        net_profit_ust = await self.client.gamm.get_best_amount_out(
            best_param["net_profit"], self.UST
        )
        if net_profit_ust < MIN_PROFIT_UST:
            raise UnprofitableArbitrage(f"Low profitability: USD {net_profit_ust.amount:.2f}")

        return ArbParams(
            timestamp_found=time.time(),
            block_found=height,
            initial_balance=initial_balance,
            route=best_param["route"],
            reverse=best_param["reverse"],
            initial_amount=best_param["initial_amount"],
            msgs=best_param["msgs"],
            n_repeat=best_param["n_repeat"],
            est_final_amount=best_param["final_amount"],
            est_fee=best_param["fee"],
            est_net_profit_usd=net_profit_ust.amount,
        )

    async def _get_params_single_route(
        self,
        route: RoutePools,
        initial_balance: OsmosisTokenAmount,
    ) -> dict:
        reverse = await route.should_reverse(self.min_start_amount)
        initial_amount = await self._get_optimal_argitrage_amount(route, reverse)
        final_amount, msgs = await route.op_swap_exact_in(initial_amount, reverse=reverse)
        single_initial_amount, n_repeat = self._check_repeats(initial_amount, initial_balance)
        if n_repeat > 1:
            _, msgs = await route.op_swap_exact_in(single_initial_amount, reverse=reverse)
        try:
            fee = await self.client.tx.estimate_fee(
                msgs,
                gas_adjustment=self.gas_adjustment,
                use_fallback_estimate=self._simulating_reserve_changes,
                estimated_gas_use=self.estimated_gas_use,
                fee_denom=self.fee_denom,
            )
        except LCDResponseError as e:
            self.log.debug(
                "Error when estimating fee",
                extra={"data": {"msgs": [msg.to_data() for msg in msgs]}},
                exc_info=True,
            )
            raise FeeEstimationError(e)

        (coin_fee,) = fee.amount
        token_fee = OsmosisNativeToken(coin_fee.denom)
        gas_cost = token_fee.to_amount(int_amount=str(coin_fee.amount)) * n_repeat
        gas_cost_raw = gas_cost / self.client.gas_adjustment
        net_profit = final_amount - initial_amount - gas_cost_raw
        return {
            "route": route,
            "reverse": reverse,
            "initial_amount": initial_amount,
            "msgs": msgs,
            "n_repeat": n_repeat,
            "final_amount": final_amount,
            "fee": fee,
            "net_profit": net_profit,
        }

    async def _get_optimal_argitrage_amount(
        self,
        route: RoutePools,
        reverse: bool,
    ) -> OsmosisTokenAmount:
        profit = await self._get_gross_profit(self.min_start_amount, route, reverse)
        if profit < 0:
            raise UnprofitableArbitrage("No profitability")
        func = partial(self._get_gross_profit_dec, route=route, reverse=reverse)
        amount, _ = await utils.aoptimization.optimize(
            func,
            x0=self.min_start_amount.amount,
            dx=self.min_start_amount.dx,
            tol=self.optimization_tolerance.amount,
        )
        return self.start_token.to_amount(amount)

    async def _get_gross_profit(
        self,
        amount_in: OsmosisTokenAmount,
        route: RoutePools,
        reverse: bool,
    ) -> OsmosisTokenAmount:
        amount_out = await route.get_amount_out_swap_exact_in(
            amount_in, reverse, safety_margin=False
        )
        return amount_out - amount_in

    async def _get_gross_profit_dec(
        self,
        amount_in: Decimal,
        route: RoutePools,
        reverse: bool,
    ) -> Decimal:
        token_amount = self.start_token.to_amount(amount_in)
        return (await self._get_gross_profit(token_amount, route, reverse)).amount

    def _check_repeats(
        self,
        initial_amount: OsmosisTokenAmount,
        initial_balance: OsmosisTokenAmount,
    ) -> tuple[OsmosisTokenAmount, int]:
        max_amount = min(initial_balance.amount, self.max_single_arbitrage.amount)
        n_repeat = math.ceil(initial_amount.amount / max_amount)
        if n_repeat > MAX_N_REPEATS:
            self.log.warning(f"{n_repeat=} is too hight, reducing to {MAX_N_REPEATS}")
            n_repeat = MAX_N_REPEATS
        return initial_amount / n_repeat, n_repeat

    async def _extract_returns_from_info(
        self,
        info: TxInfo,
    ) -> tuple[OsmosisTokenAmount, Decimal]:
        raise NotImplementedError
        balance_changes = OsmosisClient.extract_coin_balance_changes(info.logs)
        arb_changes = balance_changes[self.client.address]
        amount_out = max(change for change in arb_changes if change.token == self.start_token)
        profit = sum(
            (change for change in arb_changes if change.token == self.start_token),
            start=self.start_token.to_amount(0),
        )
        return amount_out, profit.amount


async def run(max_n_blocks: int = None):
    async with OsmosisClient() as client:
        arb_routes = await get_arbitrages(client)
        mempool_filters = get_filters(arb_routes)
        await run_strategy(
            client, arb_routes, mempool_filters, CacheGroup.OSMOSIS, max_n_blocks
        )
