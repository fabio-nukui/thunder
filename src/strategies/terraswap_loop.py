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

from .common.terra_single_tx_arbitrage import TerraArbParams, TerraSingleTxArbitrage

log = logging.getLogger(__name__)

MIN_PROFIT_UST = UST.to_amount(2)
MIN_START_AMOUNT = UST.to_amount(50)
OPTIMIZATION_TOLERANCE = UST.to_amount("0.01")
MIN_UST_RESERVED_AMOUNT = 5
FALLBACK_FEE = StdFee(gas=2150947, amount=Coins("2392410uusd"))
TOKEN_SYMBOLS: list[str] = ["TWD", "SPEC", "MIR", "STT", "MINE", "ANC", "LOTA", "ALTE"]
MAX_SLIPPAGE = Decimal("0.001")


class Direction(str, Enum):
    terraswap_first = "terraswap_first"
    loop_first = "loop_first"


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


class TerraswapLoopArbitrage(TerraSingleTxArbitrage):
    def __init__(
        self,
        client: TerraClient,
        terraswap_pair: terraswap.LiquidityPair,
        loop_pair: terraswap.LiquidityPair,
    ):
        self.terraswap_pair = terraswap_pair
        self.loop_pair = loop_pair
        self.pairs = [terraswap_pair, loop_pair]

        self.tokens = self.terraswap_pair.tokens
        self.non_ust_token = self.tokens[0] if self.tokens[1] == UST else self.tokens[1]

        self._mempool_reserve_changes = {
            pair: (pair.tokens[0].to_amount(0), pair.tokens[1].to_amount(0)) for pair in self.pairs
        }
        self._simulating_reserve_changes = False

        super().__init__(client)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(tokens={self.tokens})"

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
        loop_premium = prices["loop"] / prices["terraswap"] - 1
        if loop_premium > 0:
            direction = Direction.loop_first
        else:
            direction = Direction.terraswap_first
        ust_balance = (await UST.get_balance(self.client)).amount

        async with self._simulate_reserve_changes(filtered_mempool):
            initial_amount = await self._get_optimal_argitrage_amount(
                direction, loop_premium, ust_balance
            )
            final_amount, msgs = await self._op_arbitrage(
                initial_amount, direction, safety_round=True
            )
            try:
                fee = await self.client.tx.estimate_fee(msgs)
            except LCDResponseError as e:
                if self._simulating_reserve_changes or "account sequence mismatch" in e.message:
                    fee = FALLBACK_FEE
                else:
                    log.debug(
                        "Error when estimating fee",
                        extra={
                            "data": {
                                "loop_premium": f"{loop_premium:.3%}",
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
                f"Low profitability: USD {net_profit_ust:.2f}, {loop_premium=:0.3%}"
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
        (terraswap_reserves, loop_reserves) = await asyncio.gather(
            self.terraswap_pair.get_reserves(),
            self.loop_pair.get_reserves(),
        )
        if self.terraswap_pair.tokens[0] == UST:
            terraswap = terraswap_reserves[1].amount / terraswap_reserves[0].amount
        else:
            terraswap = terraswap_reserves[0].amount / terraswap_reserves[1].amount

        if self.loop_pair.tokens[0] == UST:
            loop = loop_reserves[1].amount / loop_reserves[0].amount
        else:
            loop = loop_reserves[0].amount / loop_reserves[1].amount

        return {"terraswap": terraswap, "loop": loop}

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
            terraswap_changes = self.terraswap_pair.simulate_reserve_change(
                self._mempool_reserve_changes[self.terraswap_pair]
            )
            loop_changes = self.loop_pair.simulate_reserve_change(
                self._mempool_reserve_changes[self.loop_pair]
            )
            try:
                async with terraswap_changes, loop_changes:
                    yield
            finally:
                self._simulating_reserve_changes = simulating_reserve_changes

    async def _get_optimal_argitrage_amount(
        self,
        direction: Direction,
        loop_premium: Decimal,
        ust_balance: Decimal,
    ) -> TerraTokenAmount:
        profit = await self._get_gross_profit(MIN_START_AMOUNT, direction)
        if profit < 0:
            raise UnprofitableArbitrage(f"No profitability, {loop_premium=:0.3%}")
        func = partial(self._get_gross_profit_dec, direction=direction)
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
        ust_amount: TerraTokenAmount,
        direction: Direction,
        safety_round: bool = False,
    ) -> TerraTokenAmount:
        amount_out, _ = await self._op_arbitrage(ust_amount, direction, safety_round)
        return amount_out - ust_amount

    async def _get_gross_profit_dec(
        self,
        amount: Decimal,
        direction: Direction,
        safety_round: bool = False,
    ) -> Decimal:
        ust_amount = UST.to_amount(amount)
        return (await self._get_gross_profit(ust_amount, direction, safety_round)).amount

    async def _op_arbitrage(
        self,
        ust_amount: TerraTokenAmount,
        direction: Direction,
        safety_round: bool,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        if direction == Direction.terraswap_first:
            token_amount, msgs_0 = await self.terraswap_pair.op_swap(
                self.client.address, ust_amount, MAX_SLIPPAGE, safety_round
            )
            amount_out, msgs_1 = await self.loop_pair.op_swap(
                self.client.address, token_amount, MAX_SLIPPAGE, safety_round
            )
        else:
            token_amount, msgs_0 = await self.loop_pair.op_swap(
                self.client.address, ust_amount, MAX_SLIPPAGE, safety_round
            )
            amount_out, msgs_1 = await self.terraswap_pair.op_swap(
                self.client.address, token_amount, MAX_SLIPPAGE, safety_round
            )
        msgs = msgs_0 + msgs_1
        return amount_out, msgs

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


async def run():
    client = await TerraClient.new()
    terraswap_factory = await terraswap.TerraswapFactory.new(client)
    loop_factory = await terraswap.LoopFactory.new(client)

    arb_routes: list[TerraswapLoopArbitrage] = []
    for symbol in TOKEN_SYMBOLS:
        name = f"{symbol}_UST"
        name_reversed = f"UST_{symbol}"
        if name in terraswap_factory.addresses["pairs"]:
            terraswap_pair = terraswap_factory.get_pair(name)
        else:
            terraswap_pair = terraswap_factory.get_pair(name_reversed)
        if name in loop_factory.addresses["pairs"]:
            loop_pair = loop_factory.get_pair(name)
        else:
            loop_pair = loop_factory.get_pair(name_reversed)
        arb_pair = await asyncio.gather(terraswap_pair, loop_pair)
        arb_routes.append(TerraswapLoopArbitrage(client, *arb_pair))

    mempool_filters = {
        pair: FilterSingleSwapTerraswapPair(pair)
        for arb_route in arb_routes
        for pair in arb_route.pairs
    }
    async for height, filtered_mempool in client.mempool.iter_height_mempool(mempool_filters):
        if any(height > arb_route.last_height_run for arb_route in arb_routes):
            utils.cache.clear_caches(utils.cache.CacheGroup.TERRA)
        for arb_route in arb_routes:
            fitered_route_mempool = {
                pair: filter_
                for pair, filter_ in filtered_mempool.items()
                if pair in arb_route.pairs
            }
            any_new_mempool_msg = any(list_msgs for list_msgs in fitered_route_mempool.values())
            if height > arb_route.last_height_run or any_new_mempool_msg:
                await arb_route.run(height, fitered_route_mempool)
