from __future__ import annotations

import asyncio
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
from chains.terra import (
    LUNA,
    UST,
    TerraClient,
    TerraNativeToken,
    TerraToken,
    TerraTokenAmount,
    terraswap,
)
from chains.terra.tx_filter import FilterSingleSwapTerraswapPair
from exceptions import TxError, UnprofitableArbitrage

from .common.default_params import (
    MAX_SLIPPAGE,
    MIN_PROFIT_UST,
    MIN_START_AMOUNT,
    OPTIMIZATION_TOLERANCE,
)
from .common.terra_single_tx_arbitrage import TerraArbParams, TerraSingleTxArbitrage, run_strategy
from .common.terraswap_lp_reserve_simulation import TerraswapLPReserveSimulationMixin

log = logging.getLogger(__name__)
ESTIMATED_GAS_USE = 1_555_000


class Direction(str, Enum):
    remove_liquidity_first = "remove_liquidity_first"
    swap_first = "swap_first"


@dataclass
class ArbParams(TerraArbParams):
    timestamp_found: float
    block_found: int

    prices: dict[TerraToken, Decimal]
    prices_denom: TerraNativeToken
    lp_tower_reserves: tuple[TerraTokenAmount, TerraTokenAmount]
    pool_0_lp_balance: TerraTokenAmount
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
            "prices": {token.symbol: float(price) for token, price in self.prices.items()},
            "prices_denom": self.prices_denom.denom,
            "lp_tower_reserves": [reserve.to_data() for reserve in self.lp_tower_reserves],
            "pool_0_lp_balance": self.pool_0_lp_balance.to_data(),
            "direction": self.direction,
            "initial_amount": self.initial_amount.to_data(),
            "msgs": [msg.to_data() for msg in self.msgs],
            "est_final_amount": self.est_final_amount.to_data(),
            "est_fee": self.est_fee.to_data(),
            "est_net_profit_usd": float(self.est_net_profit_usd),
        }


async def get_arbitrages(client: TerraClient) -> list[LPTowerArbitrage]:
    factory = await terraswap.TerraswapFactory.new(client)

    pool_0, pool_1, pool_tower = await factory.get_pairs(
        ["BLUNA_LUNA", "UST_LUNA", "BLUNA-LUNA_UST-LUNA"]
    )
    return [LPTowerArbitrage(client, pool_0, pool_1, pool_tower)]


def get_filters(
    arb_routes: list[LPTowerArbitrage],
) -> dict[terraswap.LiquidityPair, FilterSingleSwapTerraswapPair]:
    return {
        pair: FilterSingleSwapTerraswapPair(pair)
        for arb_route in arb_routes
        for pair in arb_route.pairs
    }


class LPTowerArbitrage(TerraswapLPReserveSimulationMixin, TerraSingleTxArbitrage):
    def __init__(
        self,
        client: TerraClient,
        pool_0: terraswap.LiquidityPair,
        pool_1: terraswap.LiquidityPair,
        pool_tower: terraswap.LiquidityPair,
    ):
        self.pool_0 = pool_0
        self.pool_1 = pool_1
        self.pool_tower = pool_tower

        pairs = [pool_0, pool_1, pool_tower]
        super().__init__(client, pairs=pairs, filter_keys=pairs)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(client={self.client}, "
            f"pool_tower={self.pool_tower}, state={self.state})"
        )

    def _reset_mempool_params(self):
        super()._reset_mempool_params()

    async def _get_arbitrage_params(
        self,
        height: int,
        filtered_mempool: dict[Any, list[list[dict]]] = None,
    ) -> ArbParams:
        async with self._simulate_reserve_changes(filtered_mempool):
            prices = await self._get_prices()
            balance_ratio, direction = await self._get_pool_balance_ratio(
                prices[self.pool_0.lp_token],
                prices[self.pool_1.lp_token],
            )
            luna_price = await self.client.oracle.get_exchange_rate(LUNA, UST)
            lp_ust_price = prices[self.pool_0.lp_token] * luna_price
            pool_0_lp_balance = await self.pool_0.lp_token.get_balance(self.client)

            initial_amount = await self._get_optimal_argitrage_amount(
                lp_ust_price,
                direction,
                pool_0_lp_balance,
                balance_ratio,
            )
            final_amount, msgs = await self._op_arbitrage(
                initial_amount, direction, safety_margin=True
            )
            try:
                fee = await self.client.tx.estimate_fee(
                    msgs,
                    use_fallback_estimate=self._simulating_reserve_changes,
                    estimated_gas_use=ESTIMATED_GAS_USE,
                )
            except LCDResponseError as e:
                log.debug(
                    "Error when estimating fee",
                    extra={
                        "data": {
                            "balance_ratio": f"{balance_ratio:.3%}",
                            "direction": direction,
                            "msgs": [msg.to_data() for msg in msgs],
                        },
                    },
                    exc_info=True,
                )
                raise TxError(e)
        gas_cost = TerraTokenAmount.from_coin(*fee.amount)
        gas_cost_raw = gas_cost.amount / self.client.lcd.gas_adjustment
        net_profit_ust = (final_amount - initial_amount).amount * lp_ust_price - gas_cost_raw
        if net_profit_ust < MIN_PROFIT_UST:
            raise UnprofitableArbitrage(
                f"Low profitability: USD {net_profit_ust:.2f}, {balance_ratio=:0.3%}"
            )

        return ArbParams(
            timestamp_found=time.time(),
            block_found=height,
            prices=prices,
            prices_denom=LUNA,
            lp_tower_reserves=await self.pool_tower.get_reserves(),
            pool_0_lp_balance=pool_0_lp_balance,
            direction=direction,
            initial_amount=initial_amount,
            msgs=msgs,
            est_final_amount=final_amount,
            est_fee=fee,
            est_net_profit_usd=net_profit_ust,
        )

    async def _get_prices(self) -> dict[TerraToken, Decimal]:
        pool_0_reserves, pool_1_reserves, pool_0_price, pool_1_price = await asyncio.gather(
            self.pool_0.get_reserves(),
            self.pool_1.get_reserves(),
            self.pool_0.get_price(LUNA),
            self.pool_1.get_price(LUNA),
        )
        bluna_price = pool_0_reserves[1].amount / pool_0_reserves[0].amount
        ust_price = pool_1_reserves[1].amount / pool_1_reserves[0].amount
        return {
            self.pool_0.lp_token: pool_0_price,
            self.pool_1.lp_token: pool_1_price,
            self.pool_0.tokens[0]: bluna_price,
            self.pool_1.tokens[0]: ust_price,
        }

    async def _get_pool_balance_ratio(
        self,
        pool_0_lp_price: Decimal,
        pool_1_lp_price: Decimal,
    ) -> tuple[Decimal, Direction]:
        pool_tower_reserves = await self.pool_tower.get_reserves()
        pool_0_reserve_value = pool_tower_reserves[0].amount * pool_0_lp_price
        pool_1_reserve_value = pool_tower_reserves[1].amount * pool_1_lp_price
        balance_ratio = pool_0_reserve_value / pool_1_reserve_value - 1
        if balance_ratio > 0:
            return balance_ratio, Direction.remove_liquidity_first
        return balance_ratio, Direction.swap_first

    async def _get_optimal_argitrage_amount(
        self,
        lp_ust_price: Decimal,
        direction: Direction,
        pool_0_lp_balance: TerraTokenAmount,
        balance_ratio: Decimal,
    ) -> TerraTokenAmount:
        initial_lp_amount = self.pool_0.lp_token.to_amount(MIN_START_AMOUNT.amount / lp_ust_price)
        profit = await self._get_gross_profit(initial_lp_amount, direction)
        if profit < 0:
            raise UnprofitableArbitrage(f"No profitability, {balance_ratio=:0.3%}")
        func = partial(self._get_gross_profit_dec, direction=direction)
        lp_amount, _ = await utils.aoptimization.optimize_bissection(
            func,
            x0=initial_lp_amount.amount,
            dx=initial_lp_amount.dx,
            tol=OPTIMIZATION_TOLERANCE.amount / lp_ust_price,
        )
        amount = self.pool_0.lp_token.to_amount(lp_amount)
        if amount > pool_0_lp_balance:
            log.warning(
                "Not enough balance for full arbitrage: "
                f"wanted {amount.amount:.6f}, have {pool_0_lp_balance.amount:.6f}"
            )
            return pool_0_lp_balance
        return amount

    async def _get_gross_profit(
        self,
        initial_lp_amount: TerraTokenAmount,
        direction: Direction,
        safety_margin: bool = False,
    ) -> TerraTokenAmount:
        amount_out, _ = await self._op_arbitrage(initial_lp_amount, direction, safety_margin)
        return amount_out - initial_lp_amount

    async def _get_gross_profit_dec(
        self,
        amount: Decimal,
        direction: Direction,
        safety_margin: bool = False,
    ) -> Decimal:
        token_amount = self.pool_0.lp_token.to_amount(amount)
        return (await self._get_gross_profit(token_amount, direction, safety_margin)).amount

    async def _op_arbitrage(
        self,
        initial_lp_amount: TerraTokenAmount,
        direction: Direction,
        safety_margin: bool,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        if direction == Direction.remove_liquidity_first:
            luna_amount, msgs_remove_single_side = await self.pool_0.op_remove_single_side(
                self.client.address, initial_lp_amount, LUNA, MAX_SLIPPAGE, safety_margin
            )
            lp_amount, msgs_add_single_side = await self.pool_1.op_add_single_side(
                self.client.address, luna_amount, MAX_SLIPPAGE, safety_margin
            )
            final_lp_amount, msgs_tower_swap = await self.pool_tower.op_swap(
                self.client.address, lp_amount, MAX_SLIPPAGE, safety_margin
            )
            msgs = msgs_remove_single_side + msgs_add_single_side + msgs_tower_swap
        else:
            lp_amount, msgs_tower_swap = await self.pool_tower.op_swap(
                self.client.address, initial_lp_amount, MAX_SLIPPAGE, safety_margin
            )
            luna_amount, msgs_remove_single_side = await self.pool_1.op_remove_single_side(
                self.client.address, lp_amount, LUNA, MAX_SLIPPAGE, safety_margin
            )
            final_lp_amount, msgs_add_single_side = await self.pool_0.op_add_single_side(
                self.client.address, luna_amount, MAX_SLIPPAGE, safety_margin
            )
            msgs = msgs_tower_swap + msgs_remove_single_side + msgs_add_single_side
        return final_lp_amount, msgs

    async def _extract_returns_from_info(
        self,
        info: TxInfo,
    ) -> tuple[TerraTokenAmount, Decimal]:
        tx_events = TerraClient.extract_log_events(info.logs)
        logs_from_contract = TerraClient.parse_from_contract_events(tx_events)
        first_event = logs_from_contract[0][self.pool_0.lp_token.contract_addr][0]
        last_event = logs_from_contract[-1][self.pool_0.lp_token.contract_addr][-1]
        assert last_event["to"] == self.client.address

        if first_event["to"] == self.pool_tower.contract_addr:  # swap first
            assert last_event["action"] == "mint"
        elif first_event["to"] == self.pool_0.contract_addr:  # remove liquidity first
            assert last_event["action"] == "transfer"
        else:
            raise Exception("Error when decoding tx info")
        first_amount = self.pool_0.lp_token.to_amount(int_amount=first_event["amount"])
        final_amount = self.pool_0.lp_token.to_amount(int_amount=last_event["amount"])

        pool_0_lp_price_luna = (await self._get_prices())[self.pool_0.lp_token]
        luna_price = await self.client.oracle.get_exchange_rate(LUNA, UST)
        pool_0_lp_price_ust = pool_0_lp_price_luna * luna_price
        increase_tokens = final_amount - first_amount

        return final_amount, round(increase_tokens.amount * pool_0_lp_price_ust, 18)


async def run(max_n_blocks: int = None):
    async with await TerraClient.new() as client:
        arb_routes = await get_arbitrages(client)
        mempool_filters = get_filters(arb_routes)
        await run_strategy(client, arb_routes, mempool_filters, max_n_blocks)
