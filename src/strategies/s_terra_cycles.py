from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from functools import partial
from typing import Sequence

from terra_sdk.core.auth import TxInfo
from terra_sdk.core.fee import Fee
from terra_sdk.core.wasm import MsgExecuteContract
from terra_sdk.exceptions import LCDResponseError

import utils
from arbitrage.terra import (
    TerraArbParams,
    TerraRepeatedTxArbitrage,
    TerraswapLPReserveSimulationMixin,
    run_strategy,
)
from chains.cosmos.terra import (
    LUNA,
    UST,
    BaseTerraLiquidityPair,
    NativeLiquidityPair,
    TerraClient,
    TerraNativeToken,
    TerraToken,
    TerraTokenAmount,
    anchor,
    nexus,
    terraswap,
)
from chains.cosmos.terra.swap_utils import MultiRoutes, SingleRoute
from chains.cosmos.terra.tx_filter import Filter, FilterNativeSwap, FilterSwapTerraswap
from exceptions import FeeEstimationError, InsufficientLiquidity, UnprofitableArbitrage
from strategies.common.default_params import (
    MAX_N_REPEATS,
    MAX_SINGLE_ARBITRAGE_AMOUNT,
    MIN_PROFIT_UST,
    MIN_START_AMOUNT,
    OPTIMIZATION_TOLERANCE,
)

log = logging.getLogger(__name__)

MIN_RESERVED_AMOUNT = UST.to_amount(30)
MIN_N_ARBITRAGES = 20
ANCHOR_MARKET_GAS_ADJUSMENT = Decimal("1.35")


class NoPairFound(Exception):
    pass


@dataclass
class ArbParams(TerraArbParams):
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

    initial_balance: TerraTokenAmount
    route: SingleRoute
    reverse: bool

    initial_amount: TerraTokenAmount
    msgs: list[MsgExecuteContract]
    n_repeat: int
    est_final_amount: TerraTokenAmount
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


async def get_arbitrages(client: TerraClient) -> list[TerraCyclesArbitrage]:
    terraswap_factory, loop_factory, anchor_market = await asyncio.gather(
        terraswap.TerraswapFactory.new(client),
        terraswap.LoopFactory.new(client),
        anchor.Market.new(client),
    )
    nexus_factory = nexus.Factory(client)
    list_route_groups: list[list[MultiRoutes]] = await asyncio.gather(
        _get_ust_native_routes(client, loop_factory, terraswap_factory),
        _get_luna_native_routes(client, terraswap_factory),
        _get_psi_routes(client, nexus_factory, [terraswap_factory, loop_factory]),
        _get_aust_routes(client, anchor_market, [terraswap_factory, loop_factory]),
        _get_ust_dex_3cycle_routes(client, [terraswap_factory, loop_factory]),
        _get_ust_loopdex_terraswap_2cycle_routes(client, loop_factory, terraswap_factory),
    )
    arbs: list[TerraCyclesArbitrage] = []
    for route_group in list_route_groups:
        for multi_routes in route_group:
            gas_adjustment = (
                ANCHOR_MARKET_GAS_ADJUSMENT if anchor_market in multi_routes.pairs else None
            )
            try:
                arbs.append(
                    await TerraCyclesArbitrage.new(client, multi_routes, gas_adjustment)
                )
            except FeeEstimationError as e:
                log.info(f"Error when initializing arbitrage with {multi_routes}: {e!r}")
    assert len(arbs) >= MIN_N_ARBITRAGES
    return arbs


def get_filters(
    arb_routes: list[TerraCyclesArbitrage],
) -> dict[terraswap.RouterLiquidityPair, Filter]:
    filters: dict[terraswap.RouterLiquidityPair, Filter] = {}
    for arb_route in arb_routes:
        for pair in arb_route.pairs:
            if not isinstance(
                pair, (terraswap.RouterNativeLiquidityPair, terraswap.LiquidityPair)
            ):
                continue
            router_addresses = {pair.router_address} if pair.router_address else set()
            filter_: Filter = FilterSwapTerraswap([pair], router_addresses)
            if isinstance(pair, NativeLiquidityPair):
                filter_ = filter_ | FilterNativeSwap([pair])
            filters[pair] = filter_  # type: ignore
    return filters


async def _get_ust_native_routes(
    client: TerraClient,
    loop_factory: terraswap.LoopFactory,
    terraswap_factory: terraswap.TerraswapFactory,
) -> list[MultiRoutes]:
    loop_pair = await loop_factory.get_pair("LUNA-UST")
    terraswap_pair = await terraswap_factory.get_pair("UST-LUNA")
    native_pair = terraswap_factory.get_native_pair((UST, LUNA))

    return [
        MultiRoutes(
            client=client,
            start_token=UST,
            list_steps=[[loop_pair, terraswap_pair], [native_pair]],
            router_address=terraswap_factory.router_address,
        )
    ]


async def _get_luna_native_routes(
    client: TerraClient,
    factory: terraswap.TerraswapFactory,
) -> list[MultiRoutes]:
    pat_token_symbol = re.compile(r"^[A-Z]+-LUNA$")

    routes: list[MultiRoutes] = []
    for pair_symbol in factory.pairs_addresses:
        if not (match := pat_token_symbol.match(pair_symbol)) or pair_symbol == "UST-LUNA":
            continue
        terraswap_pair = await factory.get_pair(match.group(), check_liquidity=False)
        if not isinstance(terraswap_pair.tokens[0], TerraNativeToken):
            continue
        native_pair = factory.get_native_pair(terraswap_pair.tokens)  # type: ignore
        list_steps: Sequence[Sequence] = [[terraswap_pair], [native_pair]]

        routes.append(
            MultiRoutes(client, LUNA, list_steps, router_address=factory.router_address)
        )
    return routes


async def _get_psi_routes(
    client: TerraClient,
    nexus_factory: nexus.Factory,
    terraswap_factories: Sequence[terraswap.Factory],
) -> list[MultiRoutes]:
    nexus_anchor_vaults = await nexus_factory.get_anchor_vaults()

    steps: Sequence[Sequence[BaseTerraLiquidityPair]]
    routes: list[MultiRoutes] = []
    for vault in nexus_anchor_vaults:
        if vault.b_token.symbol == "BETH":
            ust_b_asset_pairs, n_asset_psi_pairs, ust_psi_pairs = await asyncio.gather(
                _pairs_from_factories(terraswap_factories, "UST", vault.b_token.symbol),
                _pairs_from_factories(terraswap_factories, "Psi", vault.n_token.symbol),
                _pairs_from_factories(terraswap_factories, "UST", "Psi"),
            )
            steps = [ust_b_asset_pairs, [vault], n_asset_psi_pairs, ust_psi_pairs]
        elif vault.b_token.symbol == "BLUNA":
            asset_symbol = vault.b_token.symbol[1:]
            (
                ust_asset_pairs,
                asset_b_asset_pairs,
                n_asset_psi_pairs,
                ust_psi_pairs,
            ) = await asyncio.gather(
                _pairs_from_factories(terraswap_factories, "UST", asset_symbol),
                _pairs_from_factories(terraswap_factories, asset_symbol, vault.b_token.symbol),
                _pairs_from_factories(terraswap_factories, "Psi", vault.n_token.symbol),
                _pairs_from_factories(terraswap_factories, "UST", "Psi"),
            )
            steps = [
                ust_asset_pairs,
                asset_b_asset_pairs,
                [vault],
                n_asset_psi_pairs,
                ust_psi_pairs,
            ]
        routes.append(MultiRoutes(client, UST, steps))
    return routes


async def _get_aust_routes(
    client: TerraClient,
    anchor_market: anchor.Market,
    factories: Sequence[terraswap.Factory],
) -> list[MultiRoutes]:
    aust_pairs = await _pairs_from_factories(factories, "UST", "aUST")
    return [MultiRoutes(client, UST, [[anchor_market], aust_pairs])]


async def _get_ust_dex_3cycle_routes(
    client: TerraClient,
    factories: list[terraswap.Factory],
) -> list[MultiRoutes]:
    pat_ust_pair_symbol = re.compile(r"^(?:[a-zA-Z]+-UST|UST-[a-zA-Z]+)$")
    tasks = (
        f.get_pair(name)
        for f in factories
        for name in f.pairs_addresses
        if not pat_ust_pair_symbol.match(name)
    )
    non_ust_pairs: dict[tuple[TerraToken, TerraToken], list[terraswap.LiquidityPair]]
    non_ust_pairs = defaultdict(list)
    for pair in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(pair, terraswap.LiquidityPair):
            non_ust_pairs[pair.sorted_tokens].append(pair)

    routes: list[MultiRoutes] = []
    for tokens, pairs in non_ust_pairs.items():
        try:
            ust_first_pairs, ust_second_pairs = await asyncio.gather(
                _pairs_from_factories(factories, "UST", tokens[0].symbol),
                _pairs_from_factories(factories, tokens[1].symbol, "UST"),
            )
        except NoPairFound:
            continue
        routes.append(MultiRoutes(client, UST, [ust_first_pairs, pairs, ust_second_pairs]))
    return routes


async def _get_ust_loopdex_terraswap_2cycle_routes(
    client: TerraClient,
    loop_factory: terraswap.LoopFactory,
    terraswap_factory: terraswap.TerraswapFactory,
) -> list[MultiRoutes]:
    pat_token_symbol = re.compile(r"([A-Z]+)-UST|UST-([A-Z]+)")
    pair_symbol: str

    routes: list[MultiRoutes] = []
    for pair_symbol in loop_factory.pairs_addresses:
        if not (match := pat_token_symbol.match(pair_symbol)):
            continue
        reversed_symbol = f"{match.group(2)}-UST" if match.group(2) else f"UST-{match.group(1)}"
        if pair_symbol in terraswap_factory.pairs_addresses:
            terraswap_pair_symbol = pair_symbol
        elif reversed_symbol in terraswap_factory.pairs_addresses:
            terraswap_pair_symbol = reversed_symbol
        else:
            continue
        try:
            terraswap_pair, loop_pair = await asyncio.gather(
                terraswap_factory.get_pair(terraswap_pair_symbol),
                loop_factory.get_pair(pair_symbol),
            )
        except InsufficientLiquidity:
            continue
        routes.append(MultiRoutes(client, UST, [[terraswap_pair], [loop_pair]]))
    return routes


async def _pairs_from_factories(
    terraswap_factories: Sequence[terraswap.Factory],
    symbol_0: str,
    symbol_1: str,
) -> list[terraswap.LiquidityPair]:
    pairs = []
    for pair_symbol in (f"{symbol_0}-{symbol_1}", f"{symbol_1}-{symbol_0}"):
        for factory in terraswap_factories:
            if pair_symbol in factory.pairs_addresses:
                try:
                    pairs.append(await factory.get_pair(pair_symbol))
                except InsufficientLiquidity:
                    continue
    if not pairs:
        raise NoPairFound(f"No pair found for {symbol_0}-{symbol_1}")
    return pairs


class TerraCyclesArbitrage(TerraswapLPReserveSimulationMixin, TerraRepeatedTxArbitrage):
    multi_routes: MultiRoutes
    gas_adjustment: Decimal | None
    routes: list[SingleRoute]
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
        multi_routes: MultiRoutes,
        gas_adjustment: Decimal = None,
    ) -> TerraCyclesArbitrage:
        """Arbitrage with UST as starting point and a cycle of liquidity pairs"""
        assert isinstance(multi_routes.tokens[0], TerraNativeToken) and multi_routes.is_cycle

        self = super().__new__(cls)

        self.multi_routes = multi_routes
        self.start_token = multi_routes.tokens[0]
        self.gas_adjustment = gas_adjustment
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
            routes=multi_routes.routes,
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
            try:
                _, msgs = await route.op_swap(
                    self.min_start_amount, min_amount_out=self.start_token.to_amount(0)
                )
                fee = await self.client.tx.estimate_fee(msgs)
            except Exception as e:
                raise FeeEstimationError(e)
            list_gas.append(fee.gas_limit)
        return max(list_gas)

    async def _get_arbitrage_params(
        self,
        height: int,
        filtered_mempool: dict[BaseTerraLiquidityPair, list[list[dict]]] = None,
    ) -> ArbParams:
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
        route: SingleRoute,
        initial_balance: TerraTokenAmount,
    ) -> dict:
        reverse = await route.should_reverse(self.min_start_amount)
        initial_amount = await self._get_optimal_argitrage_amount(route, reverse)
        final_amount, msgs = await route.op_swap(initial_amount, reverse)
        single_initial_amount, n_repeat, capped_amount = self._check_repeats(
            initial_amount, initial_balance
        )
        if capped_amount:
            initial_amount = single_initial_amount
        if n_repeat > 1 or capped_amount:
            _, msgs = await route.op_swap(single_initial_amount, reverse)
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
        route: SingleRoute,
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
        route: SingleRoute,
        reverse: bool,
    ) -> TerraTokenAmount:
        amount_out = await route.get_swap_amount_out(amount_in, reverse, safety_margin=False)
        return amount_out - amount_in

    async def _get_gross_profit_dec(
        self,
        amount_in: Decimal,
        route: SingleRoute,
        reverse: bool,
    ) -> Decimal:
        token_amount = self.start_token.to_amount(amount_in)
        return (await self._get_gross_profit(token_amount, route, reverse)).amount

    def _check_repeats(
        self,
        initial_amount: TerraTokenAmount,
        initial_balance: TerraTokenAmount,
    ) -> tuple[TerraTokenAmount, int, bool]:
        available_amount = initial_balance - self.min_reserved_amount
        if not self.use_router:
            if initial_amount > available_amount:
                symbol = self.start_token.symbol
                self.log.info(
                    "Not enough balance for full arbitrage: "
                    f"wanted {symbol} {initial_amount.amount:,.2f}, "
                    f"have {symbol} {available_amount.amount:,.2f}"
                )
                return available_amount, 1, True
            return initial_amount, 1, False
        max_amount = min(available_amount.amount, self.max_single_arbitrage.amount)
        n_repeat = math.ceil(initial_amount.amount / max_amount)
        if n_repeat > MAX_N_REPEATS:
            self.log.warning(f"{n_repeat=} is too hight, reducing to {MAX_N_REPEATS}")
            n_repeat = MAX_N_REPEATS
        return initial_amount / n_repeat, n_repeat, False

    async def _extract_returns_from_info(
        self,
        info: TxInfo,
    ) -> tuple[TerraTokenAmount, Decimal]:
        balance_changes = TerraClient.extract_coin_balance_changes(info.logs)
        arb_changes = balance_changes[self.client.address]
        amount_out = max(change for change in arb_changes if change.token == self.start_token)
        profit = sum(
            (change for change in arb_changes if change.token == self.start_token),
            start=self.start_token.to_amount(0),
        )
        return amount_out, profit.amount


async def run(max_n_blocks: int = None):
    async with TerraClient() as client:
        arb_routes = await get_arbitrages(client)
        mempool_filters = get_filters(arb_routes)
        await run_strategy(client, arb_routes, mempool_filters, max_n_blocks)
