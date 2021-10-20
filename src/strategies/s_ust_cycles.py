from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from functools import partial

from terra_sdk.core.auth import StdFee, TxInfo
from terra_sdk.core.wasm import MsgExecuteContract
from terra_sdk.exceptions import LCDResponseError

import utils
from arbitrage.terra import (
    TerraArbParams,
    TerraSingleTxArbitrage,
    TerraswapLPReserveSimulationMixin,
    run_strategy,
)
from chains.terra import UST, TerraClient, TerraTokenAmount, terraswap
from chains.terra.tx_filter import FilterSingleSwapTerraswapPair
from exceptions import TxError, UnprofitableArbitrage

from .common.default_params import (
    MAX_SLIPPAGE,
    MIN_PROFIT_UST,
    MIN_START_AMOUNT,
    MIN_UST_RESERVED_AMOUNT,
    OPTIMIZATION_TOLERANCE,
)

log = logging.getLogger(__name__)


def _estimated_gas_use(n_steps: int) -> int:
    return 486_319 + (n_steps - 2) * 341_002


@dataclass
class ArbParams(TerraArbParams):
    timestamp_found: float
    block_found: int

    ust_balance: Decimal
    route: terraswap.SingleRoute
    reverse: bool

    initial_amount: TerraTokenAmount
    msgs: list[MsgExecuteContract]
    est_final_amount: TerraTokenAmount
    est_fee: StdFee
    est_net_profit_usd: Decimal

    def to_data(self) -> dict:
        return {
            "timestamp_found": self.timestamp_found,
            "block_found": self.block_found,
            "ust_balance": float(self.ust_balance),
            "route": str(self.route),
            "reverse": self.reverse,
            "initial_amount": self.initial_amount.to_data(),
            "msgs": [msg.to_data() for msg in self.msgs],
            "est_final_amount": self.est_final_amount.to_data(),
            "est_fee": self.est_fee.to_data(),
            "est_net_profit_usd": float(self.est_net_profit_usd),
        }


async def get_arbitrages(client: TerraClient) -> list[UstCyclesArbitrage]:
    terraswap_factory, loop_factory = await asyncio.gather(
        terraswap.TerraswapFactory.new(client), terraswap.LoopFactory.new(client)
    )
    list_routes = await asyncio.gather(
        _get_terraswap_priority_3cycle_routes(client, terraswap_factory),
        _get_ust_loop_3cycle_routes(client, loop_factory, terraswap_factory),
        _get_ust_2cycle_routes(client, loop_factory, terraswap_factory),
        _get_alte_terraswap_3cycle_routes(client, terraswap_factory),
    )
    routes = [route for list_route in list_routes for route in list_route]
    arb_routes = [UstCyclesArbitrage(client, multi_routes) for multi_routes in routes]

    return arb_routes


def get_filters(
    arb_routes: list[UstCyclesArbitrage],
) -> dict[terraswap.LiquidityPair, FilterSingleSwapTerraswapPair]:
    return {
        pair: FilterSingleSwapTerraswapPair(pair)
        for arb_route in arb_routes
        for pair in arb_route.pairs
    }


async def _get_terraswap_priority_3cycle_routes(
    client: TerraClient,
    terraswap_factory: terraswap.TerraswapFactory,
) -> list[terraswap.MultiRoutes]:
    beth_ust_pair, meth_beth_pair, ust_meth_pair = await terraswap_factory.get_pairs(
        ["BETH-UST", "mETH-BETH", "UST-mETH"]
    )
    return [
        terraswap.MultiRoutes(client, UST, [[beth_ust_pair], [meth_beth_pair], [ust_meth_pair]])
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


async def _get_ust_2cycle_routes(
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


async def _get_alte_terraswap_3cycle_routes(
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


class UstCyclesArbitrage(TerraswapLPReserveSimulationMixin, TerraSingleTxArbitrage):
    def __init__(self, client: TerraClient, multi_routes: terraswap.MultiRoutes):
        """Arbitrage with UST as starting point and a cycle of liquidity pairs"""
        assert multi_routes.tokens[0] == UST and multi_routes.is_cycle

        self.multi_routes = multi_routes
        self.routes = multi_routes.routes
        self.tokens = multi_routes.tokens[1:-1]
        self.estimated_gas_use = _estimated_gas_use(multi_routes.n_steps)

        super().__init__(client, pairs=multi_routes.pairs, filter_keys=multi_routes.pairs)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"({self.multi_routes.repr_symbols}, n_routes={self.multi_routes.n_routes})"
        )

    def _reset_mempool_params(self):
        super()._reset_mempool_params()

    async def _get_arbitrage_params(
        self,
        height: int,
        filtered_mempool: dict[terraswap.LiquidityPair, list[list[dict]]] = None,
    ) -> ArbParams:
        ust_balance = (await UST.get_balance(self.client)).amount

        params: list[dict] = []
        errors: list[Exception] = []
        async with self._simulate_reserve_changes(filtered_mempool):
            for route in self.routes:
                try:
                    params.append(await self._get_params_single_route(route, ust_balance))
                except (TxError, UnprofitableArbitrage) as e:
                    errors.append(e)
        if not params:
            raise UnprofitableArbitrage(errors)
        best_param = max(params, key=lambda x: x["net_profit_ust"])
        if (net_profit_ust := best_param["net_profit_ust"]) < MIN_PROFIT_UST:
            raise UnprofitableArbitrage(f"Low profitability: USD {net_profit_ust:.2f}")

        return ArbParams(
            timestamp_found=time.time(),
            block_found=height,
            ust_balance=ust_balance,
            route=best_param["route"],
            reverse=best_param["reverse"],
            initial_amount=best_param["initial_amount"],
            msgs=best_param["msgs"],
            est_final_amount=best_param["final_amount"],
            est_fee=best_param["fee"],
            est_net_profit_usd=net_profit_ust,
        )

    async def _get_params_single_route(
        self, route: terraswap.SingleRoute, ust_balance: Decimal
    ) -> dict:
        reverse = await route.should_reverse(MIN_START_AMOUNT)
        initial_amount = await self._get_optimal_argitrage_amount(route, reverse, ust_balance)
        final_amount, msgs = await route.op_swap(
            initial_amount, reverse, MAX_SLIPPAGE, safety_margin=True
        )
        try:
            fee = await self.client.tx.estimate_fee(
                msgs,
                use_fallback_estimate=self._simulating_reserve_changes,
                estimated_gas_use=self.estimated_gas_use,
            )
        except LCDResponseError as e:
            log.debug(
                "Error when estimating fee",
                extra={"data": {"msgs": [msg.to_data() for msg in msgs]}},
                exc_info=True,
            )
            raise TxError(e)
        gas_cost = TerraTokenAmount.from_coin(*fee.amount)
        gas_cost_raw = gas_cost.amount / self.client.gas_adjustment
        net_profit_ust = (final_amount - initial_amount).amount - gas_cost_raw
        return {
            "route": route,
            "reverse": reverse,
            "initial_amount": initial_amount,
            "msgs": msgs,
            "final_amount": final_amount,
            "fee": fee,
            "net_profit_ust": net_profit_ust,
        }

    async def _get_optimal_argitrage_amount(
        self,
        route: terraswap.SingleRoute,
        reverse: bool,
        ust_balance: Decimal,
    ) -> TerraTokenAmount:
        profit = await self._get_gross_profit(MIN_START_AMOUNT, route, reverse)
        if profit < 0:
            raise UnprofitableArbitrage("No profitability")
        func = partial(self._get_gross_profit_dec, route=route, reverse=reverse)
        ust_amount, _ = await utils.aoptimization.optimize(
            func,
            x0=MIN_START_AMOUNT.amount,
            dx=MIN_START_AMOUNT.dx,
            tol=OPTIMIZATION_TOLERANCE.amount,
        )
        if ust_amount > ust_balance:
            log.warning(
                "Not enough balance for full arbitrage: "
                f"wanted UST {ust_amount:,.2f}, have UST {ust_balance:,.2f}"
            )
            return UST.to_amount(ust_balance - MIN_UST_RESERVED_AMOUNT)
        return UST.to_amount(ust_amount)

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
        token_amount = UST.to_amount(amount_in)
        return (await self._get_gross_profit(token_amount, route, reverse, safety_round)).amount

    async def _extract_returns_from_info(
        self,
        info: TxInfo,
    ) -> tuple[TerraTokenAmount, Decimal]:
        tx_events = TerraClient.extract_log_events(info.logs)
        logs_from_contract = TerraClient.parse_from_contract_events(tx_events)

        (first_msg,) = list(logs_from_contract[0].values())[0]
        assert first_msg["action"] == terraswap.Action.swap
        assert first_msg["sender"] == self.client.address
        assert first_msg["offer_asset"] == UST.denom
        amount_sent = UST.to_amount(int_amount=first_msg["offer_amount"])

        (last_msg,) = list(logs_from_contract[-1].values())[-1]
        assert last_msg["action"] == terraswap.Action.swap
        assert last_msg["receiver"] == self.client.address
        assert last_msg["ask_asset"] == UST.denom
        amount_received = UST.to_amount(
            int_amount=int(last_msg["return_amount"]) - int(last_msg["tax_amount"])
        )
        return amount_received, (amount_received - amount_sent).amount


async def run(max_n_blocks: int = None):
    async with await TerraClient.new() as client:
        arb_routes = await get_arbitrages(client)
        mempool_filters = get_filters(arb_routes)
        await run_strategy(client, arb_routes, mempool_filters, max_n_blocks)
