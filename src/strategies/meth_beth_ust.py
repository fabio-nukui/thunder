import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from functools import partial

from terra_sdk.core.auth import StdFee, TxLog
from terra_sdk.core.wasm import MsgExecuteContract
from terra_sdk.exceptions import LCDResponseError

import utils
from chains.terra import UST, TerraClient, TerraTokenAmount, terraswap
from exceptions import TxError, UnprofitableArbitrage

from .common.terra_single_tx_arbitrage import TerraArbParams, TerraSingleTxArbitrage

log = logging.getLogger(__name__)

MIN_PROFIT_UST = UST.to_amount(2)
MIN_START_AMOUNT = UST.to_amount(200)
OPTIMIZATION_TOLERANCE = UST.to_amount("0.01")
MIN_UST_RESERVED_AMOUNT = 5


class Direction(str, Enum):
    beth_first = "beth_first"
    meth_first = "meth_first"


@dataclass
class ArbParams(TerraArbParams):
    timestamp_found: float
    block_found: int

    prices: dict[str, Decimal]
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
            "direction": self.direction,
            "initial_amount": self.initial_amount.to_data(),
            "msgs": [msg.to_data() for msg in self.msgs],
            "est_final_amount": self.est_final_amount.to_data(),
            "est_fee": self.est_fee.to_data(),
            "est_net_profit_usd": float(self.est_net_profit_usd),
        }


class MethBethUstStrategy(TerraSingleTxArbitrage):
    def __init__(
        self,
        client: TerraClient,
        meth_beth_pair: terraswap.LiquidityPair,
        beth_ust_pair: terraswap.LiquidityPair,
        ust_meth_pair: terraswap.LiquidityPair,
        router: terraswap.Router,
    ):
        self.router = router
        self.meth_beth_pair = meth_beth_pair
        self.beth_ust_pair = beth_ust_pair
        self.ust_meth_pair = ust_meth_pair

        mETH, bETH = meth_beth_pair.tokens
        self._route_meth_first: list[terraswap.RouteStep] = [
            terraswap.RouteStepTerraswap(UST, mETH),
            terraswap.RouteStepTerraswap(mETH, bETH),
            terraswap.RouteStepTerraswap(bETH, UST),
        ]
        self._route_beth_first: list[terraswap.RouteStep] = [
            terraswap.RouteStepTerraswap(UST, bETH),
            terraswap.RouteStepTerraswap(bETH, mETH),
            terraswap.RouteStepTerraswap(mETH, UST),
        ]

        super().__init__(client)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(client={self.client}, state={self.state})"

    async def _get_arbitrage_params(self, height: int, mempool: dict = None) -> ArbParams:
        if mempool:
            raise NotImplementedError
        prices = await self._get_prices()
        meth_premium = prices["meth_beth"] / (prices["meth_ust"] / prices["beth_ust"]) - 1
        if meth_premium > 0:
            direction = Direction.meth_first
            route = self._route_meth_first
        else:
            direction = Direction.beth_first
            route = self._route_beth_first
        ust_balance = (await UST.get_balance(self.client)).amount

        initial_amount = await self._get_optimal_argitrage_amount(route, meth_premium, ust_balance)
        final_amount, msgs = await self._op_arbitrage(initial_amount, route, safety_round=True)
        try:
            fee = await self.client.tx.estimate_fee(msgs)
        except LCDResponseError as e:
            log.debug(
                "Error when estimating fee",
                extra={
                    "data": {
                        "meth_premium": f"{meth_premium:.3%}",
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
                f"Low profitability: USD {net_profit_ust:.2f}, {meth_premium=:0.3%}"
            )

        return ArbParams(
            timestamp_found=time.time(),
            block_found=height,
            prices=prices,
            ust_balance=ust_balance,
            direction=direction,
            initial_amount=initial_amount,
            msgs=msgs,
            est_final_amount=final_amount,
            est_fee=fee,
            est_net_profit_usd=net_profit_ust,
        )

    async def _get_prices(self) -> dict[str, Decimal]:
        (
            meth_beth_pair_reserves,
            beth_ust_pair_reserves,
            ust_meth_pair_reserves,
        ) = await asyncio.gather(
            self.meth_beth_pair.get_reserves(),
            self.beth_ust_pair.get_reserves(),
            self.ust_meth_pair.get_reserves(),
        )

        meth_beth = meth_beth_pair_reserves[1].amount / meth_beth_pair_reserves[0].amount
        beth_ust = beth_ust_pair_reserves[1].amount / beth_ust_pair_reserves[0].amount
        meth_ust = ust_meth_pair_reserves[0].amount / ust_meth_pair_reserves[1].amount

        return {
            "meth_beth": meth_beth,
            "beth_ust": beth_ust,
            "meth_ust": meth_ust,
        }

    async def _get_optimal_argitrage_amount(
        self,
        route: list[terraswap.RouteStep],
        meth_premium: Decimal,
        ust_balance: Decimal,
    ) -> TerraTokenAmount:
        profit = await self._get_gross_profit(MIN_START_AMOUNT, route)
        if profit < 0:
            raise UnprofitableArbitrage(f"No profitability, {meth_premium=:0.3%}")
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

    async def _extract_returns_from_logs(
        self,
        logs: list[TxLog],
    ) -> tuple[TerraTokenAmount, Decimal]:
        tx_events = TerraClient.extract_log_events(logs)
        logs_from_contract = TerraClient.parse_from_contract_events(tx_events)
        log.debug(logs_from_contract)
        return UST.to_amount(), Decimal(0)  # TODO: implement


async def run():
    client = await TerraClient.new()
    pool_addresses = terraswap.get_addresses(client.chain_id)

    meth_beth_pair, beth_ust_pair, ust_meth_pair = await asyncio.gather(
        terraswap.LiquidityPair.new(pool_addresses["pools"]["meth_beth"], client),
        terraswap.LiquidityPair.new(pool_addresses["pools"]["beth_ust"], client),
        terraswap.LiquidityPair.new(pool_addresses["pools"]["ust_meth"], client),
    )
    router = terraswap.Router([meth_beth_pair, beth_ust_pair, ust_meth_pair], client)

    strategy = MethBethUstStrategy(client, meth_beth_pair, beth_ust_pair, ust_meth_pair, router)
    async for height in client.loop_latest_height():
        await strategy.run(height)
        utils.cache.clear_caches(utils.cache.CacheGroup.TERRA)
