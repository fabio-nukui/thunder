from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from collections import defaultdict
from contextlib import AsyncExitStack
from dataclasses import dataclass
from decimal import Decimal
from functools import partial
from typing import Any, Awaitable, Callable, Iterable, Sequence

from cosmos_sdk.core.auth import TxInfo
from cosmos_sdk.core.coins import Coins
from cosmos_sdk.core.fee import Fee
from cosmos_sdk.core.msg import Msg
from cosmos_sdk.core.tx import AuthInfo, Tx, TxBody

import utils
from arbitrage.cosmos import (
    CosmosArbParams,
    CosmosRepeatedTxArbitrage,
    LPReserveSimulationMixin,
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
    stader,
    terraswap,
)
from chains.cosmos.terra.route import MultiRoutes, RoutePools
from chains.cosmos.terra.tx_filter import Filter, FilterNativeSwap, FilterSwapTerraswap
from exceptions import FeeEstimationError, InsufficientLiquidity, UnprofitableArbitrage
from strategies.common.default_params import (
    MAX_CONCAT_REPEATS,
    MAX_N_REPEATS,
    MAX_SINGLE_ARBITRAGE_AMOUNT,
    MIN_PROFIT_UST,
    MIN_START_AMOUNT,
    OPTIMIZATION_TOLERANCE,
)
from utils.cache import CacheGroup

log = logging.getLogger(__name__)

MIN_RESERVED_AMOUNT = UST.to_amount(30)
MIN_N_ARBITRAGES = 20
ANCHOR_MARKET_GAS_ADJUSTMENT = Decimal("1.35")
FILTER_POOL_TYPES = (terraswap.LiquidityPair, terraswap.RouterNativeLiquidityPair)
SLIPPAGE_TOLERANCE_PER_CONCAT_REPEAT = Decimal("0.001")


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

    initial_balance: TerraTokenAmount
    route: RoutePools
    reverse: bool

    initial_amount: TerraTokenAmount
    msgs: list[Msg]
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
    terraswap_factory, loop_factory, anchor_market, lunax_vault = await asyncio.gather(
        terraswap.TerraswapFactory.new(client),
        terraswap.LoopFactory.new(client),
        anchor.Market.new(client),
        stader.LunaXVault.new(client),
    )
    nexus_factory = nexus.Factory(client)
    list_route_groups: list[list[MultiRoutes]] = await asyncio.gather(
        _get_ust_luna_routes(client, loop_factory, terraswap_factory),
        _get_luna_native_routes(client, terraswap_factory),
        _get_psi_routes(client, nexus_factory, [terraswap_factory, loop_factory]),
        _get_aust_routes(client, anchor_market, [terraswap_factory, loop_factory]),
        _get_stader_routes(client, lunax_vault, [terraswap_factory, loop_factory]),
        _get_ust_loopdex_terraswap_2cycle_routes(client, loop_factory, terraswap_factory),
        _get_ust_dex_3cycle_routes(client, [terraswap_factory, loop_factory]),
    )
    routes = _reorder_routes([r for route_group in list_route_groups for r in route_group])
    arbs: list[TerraCyclesArbitrage] = []
    for multi_routes in routes:
        gas_adjustment = (
            ANCHOR_MARKET_GAS_ADJUSTMENT if anchor_market in multi_routes.pools else None
        )
        get_max_single_arbitrage = (
            lunax_vault.get_max_deposit if lunax_vault in multi_routes.pools else None
        )
        arb = await _get_arb(client, multi_routes, gas_adjustment, get_max_single_arbitrage)
        if arb is not None:
            arbs.append(arb)
    assert len(arbs) >= MIN_N_ARBITRAGES
    return arbs


async def _get_arb(
    client: TerraClient,
    multi_routes: MultiRoutes,
    gas_adjustment: Decimal | None,
    get_max_single_arbitrage: Callable | None,
) -> TerraCyclesArbitrage | None:
    try:
        return await TerraCyclesArbitrage.new(
            client, multi_routes, gas_adjustment, get_max_single_arbitrage
        )
    except FeeEstimationError as e:
        if client.height != (latest_height := await client.get_latest_height()):
            utils.cache.clear_caches(CacheGroup.TERRA)
            client.height = latest_height
            return await _get_arb(
                client, multi_routes, gas_adjustment, get_max_single_arbitrage
            )
        log.warning(f"Error when initializing arbitrage with {multi_routes}: {e!r}")
        return None


def get_filters(
    arb_routes: list[TerraCyclesArbitrage],
) -> dict[terraswap.RouterLiquidityPair, Filter]:
    filters: dict[terraswap.RouterLiquidityPair, Filter] = {}
    for arb_route in arb_routes:
        for pool in arb_route.pools:
            if not isinstance(pool, FILTER_POOL_TYPES):
                continue
            router_addresses = {pool.router_address} if pool.router_address else set()
            filter_: Filter = FilterSwapTerraswap([pool], router_addresses)
            if isinstance(pool, NativeLiquidityPair):
                filter_ = filter_ | FilterNativeSwap([pool])
            filters[pool] = filter_  # type: ignore
    return filters


async def _get_ust_luna_routes(
    client: TerraClient,
    loop_factory: terraswap.LoopFactory,
    terraswap_factory: terraswap.TerraswapFactory,
) -> list[MultiRoutes]:
    loop_pair = await loop_factory.get_pair("[LUNA]-[UST]")
    terraswap_pair = await terraswap_factory.get_pair("[UST]-[LUNA]")
    native_pair = terraswap_factory.get_native_pair((UST, LUNA))

    return [
        MultiRoutes(
            client=client,
            start_token=UST,
            list_steps=[[terraswap_pair], [native_pair]],
            router_address=terraswap_factory.router_address,
        ),
        MultiRoutes(
            client=client,
            start_token=UST,
            list_steps=[[loop_pair], [terraswap_pair, native_pair]],
        ),
    ]


async def _get_luna_native_routes(
    client: TerraClient,
    factory: terraswap.TerraswapFactory,
) -> list[MultiRoutes]:
    routes: list[MultiRoutes] = []
    for pair in await _pairs_from_factories([factory], "LUNA", excluded_symbols=["UST"]):
        if not all(isinstance(token, TerraNativeToken) for token in pair.tokens):
            continue
        native_pair = factory.get_native_pair(pair.tokens)  # type: ignore
        list_steps: Sequence[Sequence] = [[pair], [native_pair]]

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

    steps: Sequence[Sequence[BaseTerraLiquidityPair]] = []
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
    routes: list[MultiRoutes] = []
    aust_ust = await _pairs_from_factories(factories, "UST", "aUST")
    routes.append(MultiRoutes(client, UST, [[anchor_market], aust_ust]))

    pairs_aust_swap, pairs_swap_ust = await asyncio.gather(
        _pairs_from_factories(factories, "aUST", "SWAP"),
        _pairs_from_factories(factories, "SWAP", "UST"),
    )

    pairs_aust_ust = [anchor_market, *aust_ust]
    routes.append(MultiRoutes(client, UST, [pairs_aust_ust, pairs_aust_swap, pairs_swap_ust]))

    pairs_swap_token, pairs_ust_token = await asyncio.gather(
        _pairs_from_factories(factories, "SWAP", excluded_symbols=["aUST", "UST"]),
        _pairs_from_factories(factories, "UST"),
    )

    intermediary_tokens = {
        token for pair in pairs_swap_token for token in pair.tokens if token.symbol != "SWAP"
    }
    for token in intermediary_tokens:
        token_ust = [p for p in pairs_ust_token if token in p.tokens]
        if not token_ust:
            continue
        swap_token = [p for p in pairs_swap_token if token in p.tokens]
        routes.append(
            MultiRoutes(client, UST, [pairs_aust_ust, pairs_aust_swap, swap_token, token_ust])
        )
    return routes


async def _get_stader_routes(
    client: TerraClient,
    lunax_vault: stader.LunaXVault,
    factories: Sequence[terraswap.Factory],
) -> list[MultiRoutes]:
    lunax_pairs = await _pairs_from_factories(factories, "LUNA", "LunaX")
    return [MultiRoutes(client, LUNA, [[lunax_vault], lunax_pairs], single_direction=True)]


async def _get_ust_dex_3cycle_routes(
    client: TerraClient,
    factories: list[terraswap.Factory],
) -> list[MultiRoutes]:
    non_ust_pairs: dict[tuple[TerraToken, TerraToken], list[terraswap.LiquidityPair]]
    non_ust_pairs = defaultdict(list)
    for pair in await _pairs_from_factories(factories, excluded_symbols=["UST", "aUST"]):
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
    pat_token_symbol = re.compile(r"\[([\w\-]+)\]-\[UST\]|\[UST\]-\[([\w\-]+)\]")

    routes: list[MultiRoutes] = []
    for pair_symbol in loop_factory.pairs_addresses:
        if not (match := pat_token_symbol.match(pair_symbol)) or "LUNA" in match.groups():
            continue
        reversed_symbol = (
            f"[{match.group(2)}]-[UST]" if match.group(2) else f"[UST]-[{match.group(1)}]"
        )
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


def _reorder_routes(routes: list[MultiRoutes]) -> list[MultiRoutes]:
    main_routes = []
    loop_luna_ust_routes = []
    alte_routes = []
    swap_routes = []
    for r in routes:
        if any(t.symbol == "SWAP" for p in r.pools for t in p.tokens):
            swap_routes.append(r)
        elif any(t.symbol == "ALTE" for p in r.pools for t in p.tokens):
            alte_routes.append(r)
        elif r.n_steps == 2 and any(
            p.factory_name == "loop" and p.repr_symbol == "LUNA/UST"
            for p in r.pools
            if isinstance(p, terraswap.LiquidityPair)
        ):
            loop_luna_ust_routes.append(r)
        else:
            main_routes.append(r)
    return main_routes + loop_luna_ust_routes + alte_routes + swap_routes


async def _pairs_from_factories(
    terraswap_factories: Sequence[terraswap.Factory],
    symbol_0: str = None,
    symbol_1: str = None,
    excluded_symbols: Iterable[str] = None,
) -> list[terraswap.LiquidityPair]:
    assert symbol_0 is None or "\\" not in symbol_0
    assert symbol_1 is None or "\\" not in symbol_1
    if symbol_0 == symbol_1 and symbol_0 is not None:
        raise NoPairFound(f"Invalid pair [{symbol_0}]-[{symbol_1}]")

    symbol_0 = symbol_0 or r"[\w\-]+"
    symbol_1 = symbol_1 or r"[\w\-]+"
    excluded_symbols = set(excluded_symbols) if excluded_symbols else set()

    pat = re.compile(fr"\[({symbol_0})\]-\[({symbol_1})\]|\[({symbol_1})\]-\[({symbol_0})\]")
    pairs = []
    for factory in terraswap_factories:
        for pair_symbol in factory.pairs_addresses:
            if match := pat.match(pair_symbol):
                if not excluded_symbols & set(match.groups()):
                    try:
                        pairs.append(await factory.get_pair(pair_symbol))
                    except InsufficientLiquidity:
                        continue
    if not pairs:
        raise NoPairFound(f"No pair found for [{symbol_0}]-[{symbol_1}]")
    return pairs


def _get_slippage_tolerance(n_repeat_total: int, n: int) -> Decimal:
    return SLIPPAGE_TOLERANCE_PER_CONCAT_REPEAT * (n_repeat_total - n)


class TerraCyclesArbitrage(LPReserveSimulationMixin, CosmosRepeatedTxArbitrage[TerraClient]):
    multi_routes: MultiRoutes
    gas_adjustment: Decimal | None
    func_max_single_arbitrage: Callable[[], Awaitable[TerraTokenAmount]] | None
    routes: list[RoutePools]
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
        func_max_single_arbitrage: Callable[[], Awaitable[TerraTokenAmount]] = None,
    ) -> TerraCyclesArbitrage:
        """Arbitrage with TerraNativeToken as starting point and a cycle of liquidity pairs"""
        assert isinstance(multi_routes.tokens[0], TerraNativeToken) and multi_routes.is_cycle

        self = super().__new__(cls)

        self.multi_routes = multi_routes
        self.start_token = multi_routes.tokens[0]
        self.gas_adjustment = gas_adjustment
        self.func_max_single_arbitrage = func_max_single_arbitrage
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
            pools=multi_routes.pools,
            routes=multi_routes.routes,
            filter_keys=multi_routes.pools,
            fee_denom=self.start_token.denom,
            cls_amount=TerraTokenAmount,
            verbose=False,
        )
        self.estimated_gas_use = await self._estimate_gas_use()
        self.log.debug("Initialized")
        return self

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"({self.multi_routes.repr_symbols}, n_routes={self.multi_routes.n_routes})"
        )

    def _reset_mempool_params(self):
        super()._reset_mempool_params()

    async def _get_max_single_arbitrage(self) -> TerraTokenAmount:
        if self.func_max_single_arbitrage is None:
            return self.max_single_arbitrage
        return await self.func_max_single_arbitrage()

    async def _estimate_gas_use(self) -> int:
        list_gas: list[int] = []
        for route in self.routes:
            try:
                _, msgs = await route.op_swap(
                    self.min_start_amount,
                    min_amount_out=self.start_token.to_amount(0),
                    # simulate=True,
                )
                fee = await self.client.tx.estimate_fee(msgs)
            except Exception as e:
                raise FeeEstimationError(e)
            list_gas.append(fee.gas_limit)
        return max(list_gas)

    async def _get_arbitrage_params(
        self,
        height: int,
        filtered_mempool: dict[Any, list[Tx]] = None,
    ) -> CosmosArbParams:
        initial_balance = await self.start_token.get_balance(self.client)
        max_single_arbitrage = await self._get_max_single_arbitrage()

        params: list[dict] = []
        errors: list[Exception] = []
        async with self._simulate_reserve_changes(filtered_mempool):
            for route in self.routes:
                try:
                    params.append(
                        await self._get_params_single_route(
                            route, initial_balance, max_single_arbitrage
                        )
                    )
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
        route: RoutePools,
        initial_balance: TerraTokenAmount,
        max_single_arbitrage: TerraTokenAmount,
    ) -> dict:
        reverse = await route.should_reverse(self.min_start_amount)
        initial_amount = await self._get_optimal_argitrage_amount(route, reverse)
        final_amount, msgs = await route.op_swap(initial_amount, reverse)
        single_initial_amount, n_repeat = self._check_repeats(
            initial_amount, initial_balance, max_single_arbitrage
        )
        estimated_gas_use = self.estimated_gas_use
        if n_repeat > 1:
            if self.use_router:
                _, msgs = await route.op_swap(single_initial_amount, reverse)
            else:
                sim_mempool: dict[Any, list[Tx]] | None
                msgs = []
                step_msgs: Sequence[Msg] = []
                async with AsyncExitStack() as stack:
                    for n in range(n_repeat):
                        if step_msgs:
                            tx = Tx(
                                body=TxBody(messages=step_msgs),  # type: ignore
                                auth_info=AuthInfo([], Fee(0, Coins())),
                                signatures=[],
                            )
                            sim_mempool = {
                                p: [tx] for p in route.pools if isinstance(p, FILTER_POOL_TYPES)
                            }
                        else:
                            sim_mempool = None
                        simulation = self._simulate_reserve_changes(sim_mempool, verbose=False)
                        await stack.enter_async_context(simulation)
                        slippage_tolerance = _get_slippage_tolerance(n_repeat, n)
                        _, step_msgs = await route.op_swap(
                            single_initial_amount,
                            reverse,
                            min_amount_out=single_initial_amount * (1 - slippage_tolerance),
                        )
                        msgs.extend(step_msgs)
                estimated_gas_use *= n_repeat
                n_repeat = 1
        fee = await self.client.tx.estimate_fee(
            msgs,
            gas_adjustment=self.gas_adjustment,
            use_fallback_estimate=self._simulating_reserve_changes,
            estimated_gas_use=estimated_gas_use,
            fee_denom=self.fee_denom,
        )
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
        route: RoutePools,
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
        route: RoutePools,
        reverse: bool,
    ) -> TerraTokenAmount:
        amount_out = await route.get_swap_amount_out(amount_in, reverse, safety_margin=False)
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
        initial_amount: TerraTokenAmount,
        initial_balance: TerraTokenAmount,
        max_single_arbitrage: TerraTokenAmount,
    ) -> tuple[TerraTokenAmount, int]:
        max_amount = min(max_single_arbitrage, initial_balance - self.min_reserved_amount)
        n_repeat = math.ceil(initial_amount.amount / max_amount.amount)
        max_repeat = MAX_N_REPEATS if self.use_router else MAX_CONCAT_REPEATS
        if n_repeat > max_repeat:
            self.log.warning(f"{n_repeat=} is too hight, reducing to {max_repeat=}")
            n_repeat = max_repeat
        if n_repeat == 1 and initial_amount > max_single_arbitrage / 2:
            n_repeat = 2
        return min(max_amount, initial_amount / n_repeat), n_repeat

    async def _extract_returns_from_info(
        self,
        info: TxInfo,
    ) -> tuple[TerraTokenAmount, Decimal]:
        balance_changes = TerraClient.get_coin_balance_changes(info.logs)
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
        await run_strategy(client, arb_routes, mempool_filters, CacheGroup.TERRA, max_n_blocks)
