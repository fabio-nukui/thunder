import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from functools import partial

from terra_sdk.core.auth import StdFee, TxInfo
from terra_sdk.core.coins import Coins
from terra_sdk.core.wasm import MsgExecuteContract
from terra_sdk.exceptions import LCDResponseError

import utils
from chains.terra import UST, TerraClient, TerraTokenAmount, terraswap
from chains.terra.tx_filter import FilterSingleSwapTerraswapPair
from exceptions import MaxSpreadAssertion, TxError, UnprofitableArbitrage

from .common.default_params import MIN_PROFIT_UST, MIN_UST_RESERVED_AMOUNT, OPTIMIZATION_TOLERANCE
from .common.single_tx_arbitrage import State
from .common.terra_single_tx_arbitrage import TerraArbParams, TerraSingleTxArbitrage

log = logging.getLogger(__name__)

MIN_START_AMOUNT = UST.to_amount(200)
FALLBACK_FEE = StdFee(gas=2150947, amount=Coins("2392410uusd"))


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


class MethBethUstArbitrage(TerraSingleTxArbitrage):
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
        self.pairs = [meth_beth_pair, beth_ust_pair, ust_meth_pair]

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
        self._mempool_reserve_changes = {
            pair: (pair.tokens[0].to_amount(0), pair.tokens[1].to_amount(0)) for pair in self.pairs
        }
        self._simulating_reserve_changes = False

        super().__init__(client)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(client={self.client}, state={self.state})"

    def _reset_mempool_params(self):
        self._mempool_reserve_changes = {
            pair: (pair.tokens[0].to_amount(0), pair.tokens[1].to_amount(0)) for pair in self.pairs
        }

    async def _get_arbitrage_params(
        self,
        height: int,
        filtered_mempool: dict[terraswap.LiquidityPair, list[list[dict]]] = None,
    ) -> ArbParams:
        prices = await self._get_prices()
        meth_premium = prices["meth_beth"] / (prices["meth_ust"] / prices["beth_ust"]) - 1
        if meth_premium > 0:
            direction = Direction.meth_first
            route = self._route_meth_first
        else:
            direction = Direction.beth_first
            route = self._route_beth_first
        ust_balance = (await UST.get_balance(self.client)).amount

        async with self._simulate_reserve_changes(filtered_mempool):
            initial_amount = await self._get_optimal_argitrage_amount(
                route, meth_premium, ust_balance
            )
            final_amount, msgs = await self._op_arbitrage(initial_amount, route, safety_round=True)
            try:
                fee = await self.client.tx.estimate_fee(msgs)
            except LCDResponseError as e:
                if self._simulating_reserve_changes:
                    fee = FALLBACK_FEE
                else:
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

    @asynccontextmanager
    async def _simulate_reserve_changes(
        self,
        filtered_mempool: dict[terraswap.LiquidityPair, list[list[dict]]] = None,
    ):
        if filtered_mempool is None:
            yield
        elif not any(list_msgs for list_msgs in filtered_mempool.values()):
            yield
        else:
            for pair, list_msgs in filtered_mempool.items():
                for (msg,) in list_msgs:  # Only txs with one message were filtered
                    try:
                        changes = await pair.get_reserves_changes_from_msg(msg["value"])
                    except MaxSpreadAssertion:
                        continue
                    self._mempool_reserve_changes[pair] = (
                        self._mempool_reserve_changes[pair][0] + changes[0],
                        self._mempool_reserve_changes[pair][1] + changes[1],
                    )
            simulating_reserve_changes = self._simulating_reserve_changes
            self._simulating_reserve_changes = True
            meth_beth_changes = self.meth_beth_pair.simulate_reserve_change(
                self._mempool_reserve_changes[self.meth_beth_pair]
            )
            beth_ust_changes = self.beth_ust_pair.simulate_reserve_change(
                self._mempool_reserve_changes[self.beth_ust_pair]
            )
            ust_meth_changes = self.ust_meth_pair.simulate_reserve_change(
                self._mempool_reserve_changes[self.ust_meth_pair]
            )
            try:
                async with meth_beth_changes, beth_ust_changes, ust_meth_changes:
                    yield
            finally:
                self._simulating_reserve_changes = simulating_reserve_changes

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

    async def _extract_returns_from_info(
        self,
        info: TxInfo,
    ) -> tuple[TerraTokenAmount, Decimal]:
        # coins_first_msg: Coins = info.tx.msg[0].coins
        # assert coins_first_msg, "First message expected to send coins"
        # amount_in = TerraTokenAmount.from_coin(coins_first_msg.to_list()[0])

        tx_events = TerraClient.extract_log_events(info.logs)
        logs_from_contract = TerraClient.parse_from_contract_events(tx_events)
        log.debug(logs_from_contract)
        return UST.to_amount(), Decimal(0)  # TODO: implement


async def run():
    client = await TerraClient.new()
    factory = await terraswap.TerraswapFactory.new(client)

    pairs = (meth_beth_pair, beth_ust_pair, ust_meth_pair) = await factory.get_pairs(
        ["mETH_BETH", "BETH_UST", "UST_mETH"]
    )
    router = factory.get_router(pairs)
    mempool_filters = {
        meth_beth_pair: FilterSingleSwapTerraswapPair(meth_beth_pair),
        beth_ust_pair: FilterSingleSwapTerraswapPair(beth_ust_pair),
        ust_meth_pair: FilterSingleSwapTerraswapPair(ust_meth_pair),
    }
    arb = MethBethUstArbitrage(client, meth_beth_pair, beth_ust_pair, ust_meth_pair, router)
    async for height, filtered_mempool in client.mempool.iter_height_mempool(mempool_filters):
        if height > arb.last_height_run:
            utils.cache.clear_caches(utils.cache.CacheGroup.TERRA)
        await arb.run(height, filtered_mempool)
        client.mempool.new_block_only = arb.state == State.waiting_confirmation
