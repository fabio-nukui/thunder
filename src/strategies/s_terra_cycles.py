from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from functools import partial
from typing import Sequence

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
from chains.terra.token import TerraNativeToken
from chains.terra.tx_filter import FilterSingleSwapTerraswapPair
from exceptions import EstimateFeeError, TxError, UnprofitableArbitrage

from .common.default_params import (
    MAX_N_REPEATS,
    MAX_SINGLE_ARBITRAGE_AMOUNT,
    MIN_PROFIT_UST,
    MIN_START_AMOUNT,
    OPTIMIZATION_TOLERANCE,
)

log = logging.getLogger(__name__)

MIN_RESERVED_AMOUNT = UST.to_amount(10)
MIN_N_ARBITRAGES = 20


@dataclass
class ArbParams(TerraArbParams):
    timestamp_found: float
    block_found: int

    initial_balance: TerraTokenAmount
    route: terraswap.SingleRoute
    reverse: bool

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


async def get_arbitrages(client: TerraClient) -> list[TerraCyclesArbitrage]:
    terraswap_factory, loop_factory = await asyncio.gather(
        terraswap.TerraswapFactory.new(client), terraswap.LoopFactory.new(client)
    )
    list_route_groups = await asyncio.gather(
        _get_ust_native_routes(client, terraswap_factory),
        _get_luna_native_routes(client, terraswap_factory),
        _get_ust_terraswap_3cycle_routes(client, terraswap_factory),
        _get_ust_loop_3cycle_routes(client, loop_factory, terraswap_factory),
        _get_ust_loopdex_terraswap_2cycle_routes(client, loop_factory, terraswap_factory),
        _get_ust_alte_3cycle_routes(client, terraswap_factory),
    )
    arbs: list[TerraCyclesArbitrage] = []
    for route_group in list_route_groups:
        for multi_routes in route_group:
            try:
                arbs.append(await TerraCyclesArbitrage.new(client, multi_routes))
            except EstimateFeeError as e:
                log.info(f"Error when initializing arbitrage with {multi_routes}: {e}")
    assert len(arbs) >= MIN_N_ARBITRAGES
    return arbs


def get_filters(
    arb_routes: list[TerraCyclesArbitrage],
) -> dict[terraswap.HybridLiquidityPair, FilterSingleSwapTerraswapPair]:
    filters: dict[terraswap.HybridLiquidityPair, FilterSingleSwapTerraswapPair] = {}
    for arb_route in arb_routes:
        for pair in arb_route.pairs:
            if not isinstance(pair, terraswap.LiquidityPair):
                continue
            filters[pair] = FilterSingleSwapTerraswapPair(pair)
    return filters


async def _get_ust_native_routes(
    client: TerraClient,
    factory: terraswap.TerraswapFactory,
) -> list[terraswap.MultiRoutes]:
    terraswap_pair = await factory.get_pair("UST-LUNA")
    native_pair = NativeLiquidityPair(client, (UST, LUNA))

    return [
        terraswap.MultiRoutes(
            client=client,
            start_token=UST,
            list_steps=[[terraswap_pair], [native_pair]],
            router_address=factory.addresses["router"],
        )
    ]


async def _get_luna_native_routes(
    client: TerraClient,
    factory: terraswap.TerraswapFactory,
) -> list[terraswap.MultiRoutes]:
    pat_token_symbol = re.compile(r"^[A-Z]+-LUNA$")

    routes: list[terraswap.MultiRoutes] = []
    for pair_symbol in factory.addresses["pairs"]:
        if not (match := pat_token_symbol.match(pair_symbol)) or pair_symbol == "UST-LUNA":
            continue
        terraswap_pair = await factory.get_pair(match.group())
        if not isinstance(terraswap_pair.tokens[0], TerraNativeToken):
            continue
        native_pair = NativeLiquidityPair(client, terraswap_pair.tokens)  # type: ignore
        list_steps: Sequence[Sequence] = [[terraswap_pair], [native_pair]]

        routes.append(
            terraswap.MultiRoutes(
                client, LUNA, list_steps, router_address=factory.addresses["router"]
            )
        )
    return routes


async def _get_ust_terraswap_3cycle_routes(
    client: TerraClient,
    terraswap_factory: terraswap.TerraswapFactory,
) -> list[terraswap.MultiRoutes]:
    beth_ust_pair, meth_beth_pair, ust_meth_pair = await terraswap_factory.get_pairs(
        ["BETH-UST", "mETH-BETH", "UST-mETH"]
    )
    return [
        terraswap.MultiRoutes(
            client=client,
            start_token=UST,
            list_steps=[[beth_ust_pair], [meth_beth_pair], [ust_meth_pair]],
            router_address=terraswap_factory.addresses["router"],
        )
    ]


async def _get_ust_loop_3cycle_routes(
    client: TerraClient,
    loop_factory: terraswap.LoopFactory,
    terraswap_factory: terraswap.TerraswapFactory,
) -> list[terraswap.MultiRoutes]:
    loop_ust_pair = await loop_factory.get_pair("LOOP-UST")
    pat_token_symbol = re.compile(r"^(?:([a-zA-Z]+)-LOOP|LOOP-([a-zA-Z]+))$")

    routes: list[terraswap.MultiRoutes] = []
    for pair_symbol in loop_factory.addresses["pairs"]:
        if not (match := pat_token_symbol.match(pair_symbol)) or pair_symbol == "LOOP-UST":
            continue
        token_symbol = match.group(1) or match.group(2)

        ust_pairs: list[terraswap.LiquidityPair] = []
        for factory in (terraswap_factory, loop_factory):
            for ust_pair_symbol in (f"{token_symbol}-UST", f"UST-{token_symbol}"):
                if ust_pair_symbol in factory.addresses["pairs"]:
                    ust_pairs.append(await factory.get_pair(ust_pair_symbol))
        assert ust_pairs, f"No UST pairs found for {token_symbol}"

        loop_token_pair = await loop_factory.get_pair(pair_symbol)
        list_steps = [ust_pairs, [loop_token_pair], [loop_ust_pair]]

        routes.append(terraswap.MultiRoutes(client, UST, list_steps))
    return routes


async def _get_ust_loopdex_terraswap_2cycle_routes(
    client: TerraClient,
    loop_factory: terraswap.LoopFactory,
    terraswap_factory: terraswap.TerraswapFactory,
) -> list[terraswap.MultiRoutes]:
    pat_token_symbol = re.compile(r"([A-Z]+)-UST|UST-([A-Z]+)")
    pair_symbol: str

    routes: list[terraswap.MultiRoutes] = []
    for pair_symbol in loop_factory.addresses["pairs"]:
        if not (match := pat_token_symbol.match(pair_symbol)):
            continue
        reversed_symbol = f"{match.group(2)}-UST" if match.group(2) else f"UST-{match.group(1)}"
        if pair_symbol in terraswap_factory.addresses["pairs"]:
            terraswap_pair = await terraswap_factory.get_pair(pair_symbol)
        elif reversed_symbol in terraswap_factory.addresses["pairs"]:
            terraswap_pair = await terraswap_factory.get_pair(reversed_symbol)
        else:
            continue
        loop_pair = await loop_factory.get_pair(pair_symbol)
        routes.append(terraswap.MultiRoutes(client, UST, [[terraswap_pair], [loop_pair]]))
    return routes


async def _get_ust_alte_3cycle_routes(
    client: TerraClient,
    factory: terraswap.TerraswapFactory,
) -> list[terraswap.MultiRoutes]:
    alte_ust_pair = await factory.get_pair("ALTE-UST")
    pat_token_symbol = re.compile(r"^(?:([a-zA-Z]+)-ALTE|ALTE-([a-zA-Z]+))$")

    routes: list[terraswap.MultiRoutes] = []
    for pair_symbol in factory.addresses["pairs"]:
        if not (match := pat_token_symbol.match(pair_symbol)) or pair_symbol == "ALTE-UST":
            continue
        token_symbol = match.group(1) or match.group(2)

        ust_pairs: list[terraswap.LiquidityPair] = []
        for ust_pair_symbol in (f"{token_symbol}-UST", f"UST-{token_symbol}"):
            if ust_pair_symbol in factory.addresses["pairs"]:
                ust_pairs.append(await factory.get_pair(ust_pair_symbol))
        assert ust_pairs, f"No UST pairs found for {token_symbol}"

        alte_token_pair = await factory.get_pair(pair_symbol)
        list_steps = [ust_pairs, [alte_token_pair], [alte_ust_pair]]
        routes.append(terraswap.MultiRoutes(client, UST, list_steps))
    return routes


class TerraCyclesArbitrage(TerraswapLPReserveSimulationMixin, TerraRepeatedTxArbitrage):
    multi_routes: terraswap.MultiRoutes
    routes: list[terraswap.SingleRoute]
    start_token: TerraNativeToken
    use_router: bool
    estimated_gas_use: int
    min_start_amount: TerraTokenAmount
    min_reserved_amount: TerraTokenAmount
    max_single_arbitrage: TerraTokenAmount
    optimization_tolerance: TerraTokenAmount

    @classmethod
    async def new(
        cls,
        client: TerraClient,
        multi_routes: terraswap.MultiRoutes,
    ) -> TerraCyclesArbitrage:
        """Arbitrage with UST as starting point and a cycle of liquidity pairs"""
        assert isinstance(multi_routes.tokens[0], TerraNativeToken) and multi_routes.is_cycle

        self = super().__new__(cls)

        self.multi_routes = multi_routes
        self.routes = multi_routes.routes
        self.start_token = multi_routes.tokens[0]
        self.use_router = multi_routes.router_address is not None

        (
            self.min_start_amount,
            self.min_reserved_amount,
            self.max_single_arbitrage,
            self.optimization_tolerance,
        ) = await asyncio.gather(
            client.market.compute_swap_no_spread(MIN_START_AMOUNT, self.start_token),
            client.market.compute_swap_no_spread(MIN_RESERVED_AMOUNT, self.start_token),
            client.market.compute_swap_no_spread(MAX_SINGLE_ARBITRAGE_AMOUNT, self.start_token),
            client.market.compute_swap_no_spread(OPTIMIZATION_TOLERANCE, self.start_token),
        )

        self.__init__(
            client,
            pairs=multi_routes.pairs,
            filter_keys=multi_routes.pairs,
            fee_denom=self.start_token.denom,
        )
        self.estimated_gas_use = await self._estimate_gas_use()
        return self

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"({self.multi_routes.repr_symbols}, n_routes={self.multi_routes.n_routes})"
        )

    def _reset_mempool_params(self):
        super()._reset_mempool_params()

    async def _estimate_gas_use(self) -> int:
        list_gas: list[int] = []
        for route in self.routes:
            _, msgs = await route.op_swap(
                self.min_start_amount, min_amount_out=self.start_token.to_amount(0)
            )
            try:
                fee = await self.client.tx.estimate_fee(msgs)
            except Exception as e:
                raise EstimateFeeError(e)
            list_gas.append(fee.gas)
        return max(list_gas)

    async def _get_arbitrage_params(
        self,
        height: int,
        filtered_mempool: dict[terraswap.HybridLiquidityPair, list[list[dict]]] = None,
    ) -> ArbParams:
        initial_balance = await self.start_token.get_balance(self.client)

        params: list[dict] = []
        errors: list[Exception] = []
        async with self._simulate_reserve_changes(filtered_mempool):
            for route in self.routes:
                try:
                    params.append(await self._get_params_single_route(route, initial_balance))
                except (TxError, UnprofitableArbitrage) as e:
                    errors.append(e)
        if not params:
            raise UnprofitableArbitrage(errors)
        best_param = max(params, key=lambda x: x["net_profit"])
        net_profit_ust = await self.client.market.compute_swap_no_spread(
            best_param["net_profit"], UST
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
        route: terraswap.SingleRoute,
        initial_balance: TerraTokenAmount,
    ) -> dict:
        reverse = await route.should_reverse(self.min_start_amount)
        initial_amount = await self._get_optimal_argitrage_amount(route, reverse)
        final_amount, msgs = await route.op_swap(initial_amount, reverse)
        single_initial_amount, n_repeat = self._check_repeats(initial_amount, initial_balance)
        if n_repeat > 1:
            _, msgs = await route.op_swap(single_initial_amount, reverse)
        try:
            fee = await self.client.tx.estimate_fee(
                msgs,
                use_fallback_estimate=self._simulating_reserve_changes,
                estimated_gas_use=self.estimated_gas_use,
                fee_denom=self.fee_denom,
            )
        except LCDResponseError as e:
            log.debug(
                "Error when estimating fee",
                extra={"data": {"msgs": [msg.to_data() for msg in msgs]}},
                exc_info=True,
            )
            raise TxError(e)
        gas_cost = TerraTokenAmount.from_coin(*fee.amount) * n_repeat
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
        route: terraswap.SingleRoute,
        reverse: bool,
    ) -> TerraTokenAmount:
        profit = await self._get_gross_profit(self.min_start_amount, route, reverse)
        if profit < 0:
            raise UnprofitableArbitrage("No profitability")
        func = partial(self._get_gross_profit_dec, route=route, reverse=reverse)
        amount, _ = await utils.aoptimization.optimize(
            func,
            x0=self.min_start_amount.amount,
            dx=self.min_start_amount.dx,
            tol=OPTIMIZATION_TOLERANCE.amount,
        )
        return self.start_token.to_amount(amount)

    async def _get_gross_profit(
        self,
        amount_in: TerraTokenAmount,
        route: terraswap.SingleRoute,
        reverse: bool,
        safety_round: bool = False,
    ) -> TerraTokenAmount:
        amount_out = await route.get_swap_amount_out(amount_in, reverse, safety_round)
        return amount_out - amount_in

    async def _get_gross_profit_dec(
        self,
        amount_in: Decimal,
        route: terraswap.SingleRoute,
        reverse: bool,
        safety_round: bool = False,
    ) -> Decimal:
        token_amount = self.start_token.to_amount(amount_in)
        return (await self._get_gross_profit(token_amount, route, reverse, safety_round)).amount

    def _check_repeats(
        self,
        initial_amount: TerraTokenAmount,
        initial_balance: TerraTokenAmount,
    ) -> tuple[TerraTokenAmount, int]:
        available_amount = initial_balance - self.min_reserved_amount
        if not self.use_router:
            if initial_amount > available_amount:
                symbol = self.start_token.symbol
                log.warning(
                    "Not enough balance for full arbitrage: "
                    f"wanted {symbol} {initial_amount.amount:,.2f}, "
                    f"have {symbol} {available_amount.amount:,.2f}"
                )
                return available_amount, 1
            return initial_amount, 1
        max_amount = min(available_amount.amount, self.max_single_arbitrage.amount)
        n_repeat = math.ceil(initial_amount.amount / max_amount)
        if n_repeat > MAX_N_REPEATS:
            log.warning(f"{n_repeat=} is too hight, reducing to {MAX_N_REPEATS}")
            n_repeat = MAX_N_REPEATS
        return initial_amount / n_repeat, n_repeat

    async def _extract_returns_from_info(
        self,
        info: TxInfo,
    ) -> tuple[TerraTokenAmount, Decimal]:
        balance_changes = TerraClient.extract_coin_balance_changes(info.logs)
        arb_changes = balance_changes[self.client.address]
        assert all(change.token == self.start_token for change in arb_changes)
        assert len(arb_changes) == 2
        return max(arb_changes), arb_changes[0].amount + arb_changes[1].amount


async def run(max_n_blocks: int = None):
    async with await TerraClient.new() as client:
        arb_routes = await get_arbitrages(client)
        mempool_filters = get_filters(arb_routes)
        await run_strategy(client, arb_routes, mempool_filters, max_n_blocks)