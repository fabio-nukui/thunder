from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from functools import partial
from typing import Any, Iterable, Sequence

from cosmos_proto.osmosis.gamm.v1beta1 import Pool
from cosmos_sdk.core.auth import TxInfo
from cosmos_sdk.core.fee import Fee
from cosmos_sdk.core.gamm import SwapAmountInRoute
from cosmos_sdk.core.tx import Tx
from cosmos_sdk.core.wasm import MsgExecuteContract

import utils
from arbitrage.cosmos import (
    CosmosArbParams,
    CosmosRepeatedTxArbitrage,
    LPReserveSimulationMixin,
    run_strategy,
)
from chains.cosmos.osmosis import (
    OSMO,
    GAMMLiquidityPool,
    OsmosisClient,
    OsmosisNativeToken,
    OsmosisTokenAmount,
)
from chains.cosmos.osmosis.route import MultiRoutes, RoutePools
from chains.cosmos.osmosis.token import get_ibc_token
from chains.cosmos.osmosis.tx_filter import FilterSwap
from exceptions import FeeEstimationError, InsufficientLiquidity, UnprofitableArbitrage
from strategies.common.default_params import MAX_N_REPEATS
from utils.cache import CacheGroup, ttl_cache

log = logging.getLogger(__name__)

_START_TOKEN_PRICE_CACHE_TTL = 600

MIN_START_AMOUNT_UST = Decimal(20)
MAX_ARBITRAGE_UST = Decimal(50_000)
OPTIMIZATION_TOL_UST = Decimal("0.05")
MIN_PROFIT_UST = Decimal("0.1")
MIN_N_ARBITRAGES = 5
MIN_ROUTE_TOKEN_AMOUNT = 1 * 10 ** 6
LIQUIDITY_TEST_AMOUNT_UST = 50
MIN_ROUND_TRIP_EFFICIENCY = Decimal("0.60")
MAX_HOPS = 3


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


async def _get_atom_price() -> Decimal:
    url = "https://api.coingecko.com/api/v3/simple/price?ids=cosmos&vs_currencies=usd"
    res = await utils.ahttp.get(url)
    return Decimal(str((res.json()["cosmos"]["usd"])))


async def get_arbitrages(client: OsmosisClient) -> list[OsmosisCyclesArbitrage]:
    pools = await client.gamm.get_all_pools()
    list_route_groups: Sequence[list[MultiRoutes]] = await asyncio.gather(
        _get_token_routes(client, pools.values(), "UST"),
        _get_token_routes(client, pools.values(), "ATOM"),
    )
    arbs: list[OsmosisCyclesArbitrage] = []
    for route_group in list_route_groups:
        for multi_routes in route_group:
            try:
                arbs.append(await OsmosisCyclesArbitrage.new(client, multi_routes))
            except FeeEstimationError as e:
                log.info(f"Error when initializing arbitrage with {multi_routes}: {e!r}")
    assert len(arbs) >= MIN_N_ARBITRAGES
    return arbs


async def _get_token_routes(
    client: OsmosisClient,
    pools: Iterable[Pool],
    token_name: str,
) -> list[MultiRoutes]:
    token = get_ibc_token(token_name, client.chain_id)
    liquidity_test_amount = await _get_liquidity_test_amount(token)

    dict_routes: dict[tuple[OsmosisNativeToken, ...], list[RoutePools]] = defaultdict(list)
    for routes in _get_cycle_amount_in_routes(list(pools), token, client):
        tasks = (GAMMLiquidityPool.new(r.pool_id, client) for r in routes)
        try:
            list_pools = await asyncio.gather(*tasks)
        except InsufficientLiquidity:
            continue

        route_tokens = [OsmosisNativeToken(r.token_out_denom, client.chain_id) for r in routes]
        route_pools = RoutePools([token, *route_tokens], list_pools, client)

        amount_out = await route_pools.get_amount_out_swap_exact_in(liquidity_test_amount)
        if amount_out / liquidity_test_amount < MIN_ROUND_TRIP_EFFICIENCY:
            continue

        same_tokens_routes = dict_routes[tuple(sorted(route_tokens))]
        if not any(_same_pool_ids(r, route_pools) for r in same_tokens_routes):
            same_tokens_routes.append(route_pools)

    return [MultiRoutes(client, list_routes) for list_routes in dict_routes.values()]


def _same_pool_ids(route_0: RoutePools, route_1: RoutePools) -> bool:
    return sorted(p.pool_id for p in route_0.pools) == sorted(p.pool_id for p in route_1.pools)


async def _get_liquidity_test_amount(token: OsmosisNativeToken) -> OsmosisTokenAmount:
    if token.symbol.startswith("USD"):
        return token.to_amount(LIQUIDITY_TEST_AMOUNT_UST)
    if token.symbol.startswith("ATOM"):
        return token.to_amount(LIQUIDITY_TEST_AMOUNT_UST / await _get_atom_price())
    raise ValueError(token)


def _get_cycle_amount_in_routes(
    pools: list[Pool],
    token_in: OsmosisNativeToken,
    client: OsmosisClient,
    max_hops: int = MAX_HOPS,
    current_route: list[SwapAmountInRoute] = None,
    original_token_in: OsmosisNativeToken = None,
    final_routes: list[list[SwapAmountInRoute]] = None,
) -> list[list[SwapAmountInRoute]]:
    current_route = [] if current_route is None else current_route
    original_token_in = token_in if original_token_in is None else original_token_in
    final_routes = [] if final_routes is None else final_routes

    assert len(pools) > 0, "at least one pair must be given"
    assert max_hops > 0, "max_hops must be positive number"

    for pool in pools:
        if not any(
            (a.token.denom == token_in.denom and int(a.token.amount) > MIN_ROUTE_TOKEN_AMOUNT)
            for a in pool.pool_assets
        ):
            continue
        for asset in pool.pool_assets:
            if asset.token.denom == original_token_in.denom:
                if current_route:
                    # End of recursion
                    route = [*current_route, SwapAmountInRoute(pool.id, asset.token.denom)]
                    final_routes.append(route)
                else:
                    continue
            elif asset.token.denom != token_in.denom and max_hops > 1 and len(pools) > 1:
                token_in_ = OsmosisNativeToken(asset.token.denom, client.chain_id)
                _get_cycle_amount_in_routes(
                    pools=[p for p in pools if p is not pool],
                    token_in=token_in_,
                    client=client,
                    max_hops=max_hops - 1,
                    current_route=[*current_route, SwapAmountInRoute(pool.id, token_in_.denom)],
                    original_token_in=original_token_in,
                    final_routes=final_routes,
                )
    return final_routes


def get_filters(
    arb_routes: list[OsmosisCyclesArbitrage],
) -> dict[GAMMLiquidityPool, FilterSwap]:
    return {
        pool: FilterSwap(pool.pool_id) for arb_route in arb_routes for pool in arb_route.pools
    }


class OsmosisCyclesArbitrage(
    LPReserveSimulationMixin, CosmosRepeatedTxArbitrage[OsmosisClient]
):
    routes: Sequence[RoutePools]
    start_token: OsmosisNativeToken
    gas_adjustment: Decimal | None
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

        self.__init__(
            client,
            pools=multi_routes.pools,
            routes=multi_routes.routes,
            filter_keys=multi_routes.pools,
            fee_denom=OSMO.denom,
            cls_amount=OsmosisTokenAmount,
        )

        price = await self._get_start_token_price()
        self.min_start_amount = self.start_token.to_amount(MIN_START_AMOUNT_UST) / price
        self.max_single_arbitrage = self.start_token.to_amount(MAX_ARBITRAGE_UST) / price
        self.optimization_tolerance = self.start_token.to_amount(OPTIMIZATION_TOL_UST) / price
        self.estimated_gas_use = await self._estimate_gas_use()

        return self

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"({self.routes[0].repr_tokens}, n_routes={len(self.routes)})"
        )

    def _reset_mempool_params(self):
        super()._reset_mempool_params()

    @ttl_cache(CacheGroup.OSMOSIS, ttl=_START_TOKEN_PRICE_CACHE_TTL)
    async def _get_start_token_price(self) -> Decimal:
        if self.start_token == get_ibc_token("UST", self.client.chain_id):
            return Decimal(1)
        if self.start_token == get_ibc_token("ATOM", self.client.chain_id):
            return await _get_atom_price()
        raise ValueError(f"Unexpected {self.start_token=}")

    async def _estimate_gas_use(self) -> int:
        longest_route = max(self.routes, key=lambda x: len(x.pools))
        try:
            _, msgs = await longest_route.op_swap_exact_in(
                self.min_start_amount,
                min_amount_out=self.min_start_amount * MIN_ROUND_TRIP_EFFICIENCY,
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
        net_profit_usd = best_param["net_profit"].amount * await self._get_start_token_price()
        if net_profit_usd < MIN_PROFIT_UST:
            raise UnprofitableArbitrage(f"Low profitability: USD {net_profit_usd:.2f}")

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
            est_net_profit_usd=net_profit_usd,
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
        fee = await self.client.tx.estimate_fee(
            msgs,
            gas_adjustment=self.gas_adjustment,
            use_fallback_estimate=self._simulating_reserve_changes,
            estimated_gas_use=self.estimated_gas_use,
            fee_denom=self.fee_denom,
        )

        (coin_fee,) = fee.amount
        token_fee = OsmosisNativeToken(coin_fee.denom)
        gas_cost = token_fee.to_amount(int_amount=str(coin_fee.amount)) * n_repeat
        gas_cost_raw = gas_cost / self.client.gas_adjustment
        if gas_cost_raw:
            raise NotImplementedError(f"Not implemented for gas price > 0, {gas_cost_raw=}")
        gas_cost_converted = self.start_token.to_amount(0)
        net_profit = final_amount - initial_amount - gas_cost_converted
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
        balance_changes = OsmosisClient.get_coin_balance_changes(info.logs)
        arb_changes = balance_changes[self.client.address]
        amount_out = max(change for change in arb_changes if change.token == self.start_token)
        profit = sum(
            (change for change in arb_changes if change.token == self.start_token),
            start=self.start_token.to_amount(0),
        )
        return amount_out, profit.amount


async def run(max_n_blocks: int = None):
    async with OsmosisClient(allow_concurrent_pool_arbs=True) as client:
        arb_routes = await get_arbitrages(client)
        mempool_filters = get_filters(arb_routes)
        await run_strategy(
            client,
            arb_routes,
            mempool_filters,
            CacheGroup.OSMOSIS,
            max_n_blocks,
            verbose_decode_warnings=False,
        )
