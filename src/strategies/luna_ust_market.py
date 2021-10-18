import logging
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
from chains.terra import LUNA, UST, TerraClient, TerraTokenAmount, terraswap
from chains.terra.tx_filter import FilterSingleSwapTerraswapPair
from exceptions import TxError, UnprofitableArbitrage

from .common.default_params import MIN_PROFIT_UST, MIN_UST_RESERVED_AMOUNT, OPTIMIZATION_TOLERANCE
from .common.terra_single_tx_arbitrage import TerraArbParams, TerraSingleTxArbitrage
from .common.terraswap_lp_reserve_simulation import TerraswapLPReserveSimulationMixin

log = logging.getLogger(__name__)

MIN_START_AMOUNT = UST.to_amount(200)


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
            "est_final_amount": self.est_final_amount.to_data(),
            "est_fee": self.est_fee.to_data(),
            "est_net_profit_usd": float(self.est_net_profit_usd),
        }


class LunaUstMarketArbitrage(TerraswapLPReserveSimulationMixin, TerraSingleTxArbitrage):
    def __init__(self, client: TerraClient, router: terraswap.Router) -> None:
        self.router = router
        (self.terraswap_pool,) = router.pairs.values()
        self._route_native_first: list[terraswap.RouteStep] = [
            terraswap.RouteStepNative(UST, LUNA),
            terraswap.RouteStepTerraswap(LUNA, UST),
        ]
        self._route_terraswap_first: list[terraswap.RouteStep] = [
            terraswap.RouteStepTerraswap(UST, LUNA),
            terraswap.RouteStepNative(LUNA, UST),
        ]

        super().__init__(client, pairs=[self.terraswap_pool])

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(client={self.client}, state={self.state})"

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

            initial_amount = await self._get_optimal_argitrage_amount(
                route, terraswap_premium, ust_balance
            )
            final_amount, msgs = await self._op_arbitrage(initial_amount, route, safety_round=True)
        try:
            fee = await self.client.tx.estimate_fee(msgs)
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
        gas_cost = TerraTokenAmount.from_coin(*fee.amount)
        gas_cost_raw = gas_cost.amount / self.client.lcd.gas_adjustment
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
        ust_balance: Decimal,
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
        if ust_amount > ust_balance:
            log.warning(
                "Not enough balance for full arbitrage: "
                f"wanted UST {ust_amount:,.2f}, have UST {ust_balance:,.2f}"
            )
            return UST.to_amount(ust_balance - MIN_UST_RESERVED_AMOUNT)
        return UST.to_amount(ust_amount)

    async def _get_gross_profit(
        self,
        initial_lp_amount: TerraTokenAmount,
        route: list[terraswap.RouteStep],
        safety_round: bool = False,
    ) -> TerraTokenAmount:
        amount_out, _ = await self._op_arbitrage(initial_lp_amount, route, safety_round)
        return amount_out - initial_lp_amount

    async def _get_gross_profit_dec(
        self,
        amount: Decimal,
        route: list[terraswap.RouteStep],
        safety_round: bool = False,
    ) -> Decimal:
        token_amount = UST.to_amount(amount)
        return (await self._get_gross_profit(token_amount, route, safety_round)).amount

    async def _op_arbitrage(
        self,
        initial_ust_amount: TerraTokenAmount,
        route: list[terraswap.RouteStep],
        safety_round: bool,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        return await self.router.op_route_swap(
            self.client.address, initial_ust_amount, route, initial_ust_amount, safety_round
        )

    async def _extract_returns_from_info(
        self,
        info: TxInfo,
    ) -> tuple[TerraTokenAmount, Decimal]:
        tx_events = TerraClient.extract_log_events(info.logs)
        logs_from_contract = TerraClient.parse_from_contract_events(tx_events)
        log.debug(logs_from_contract)
        return UST.to_amount(), Decimal(0)  # TODO: implement


async def run(max_n_blocks: int = None):
    async with await TerraClient.new() as client:
        factory = await terraswap.TerraswapFactory.new(client)

        pair = await factory.get_pair("UST_LUNA")
        router = factory.get_router([pair])
        mempool_filter = {pair: FilterSingleSwapTerraswapPair(pair)}
        start_height = client.height

        arb = LunaUstMarketArbitrage(client, router)
        async for height, mempool in client.mempool.iter_height_mempool(mempool_filter):
            if height > arb.last_height_run:
                utils.cache.clear_caches(utils.cache.CacheGroup.TERRA)
            await arb.run(height, mempool)
            if max_n_blocks is not None and (n_blocks := height - start_height) >= max_n_blocks:
                break
        log.info(f"Stopped execution after {n_blocks=}")
