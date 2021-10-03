from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from functools import partial
from typing import Optional

from terra_sdk.core.auth import StdFee
from terra_sdk.core.wasm import MsgExecuteContract

import utils
from chains.terra import (LUNA, UST, TerraClient, TerraswapLiquidityPair, TerraToken,
                          TerraTokenAmount)
from chains.terra.core import TerraNativeToken
from exceptions import BlockchainNewState, IsBusy, UnprofitableArbitrage

log = logging.getLogger(__name__)

ADDR_BLUNA_LUNA_POOL = 'terra1jxazgm67et0ce260kvrpfv50acuushpjsz2y0p'
ADDR_UST_LUNA_POOL = 'terra1tndcaqxkpc5ce9qee5ggqf430mr2z3pefe5wj6'
ADDR_BLUNA_LUNA_UST_TOWER_POOL = 'terra1wrwf3um5vm30vpwnlpvjzgwpf5fjknt68nah05'
MIN_NET_PROFIT_MARGIN = 0.005
MIN_PROFIT_UST = TerraTokenAmount(UST, 1)
MIN_START_AMOUNT = TerraTokenAmount(UST, 10)
OPTIMIZATION_TOLERANCE = TerraTokenAmount(UST, '0.01')

MAX_SLIPPAGE = Decimal('0.001')


class Direction(str, Enum):
    remove_liquidity_first = 'remove_liquidity_first'
    swap_first = 'swap_first'


class ExecutionState(str, Enum):
    start = 'start'
    ready_to_broadcast = 'ready_to_broadcast'
    waiting_confirmation = 'waiting_confirmation'
    finished = 'finished'


class TxStatus(str, Enum):
    succeeded = 'succeeded'
    failed = 'failed'
    not_found = 'not_found'


@dataclass
class ArbParams:
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
    est_net_profit_ust: Decimal

    def to_data(self) -> dict:
        return {
            'timestamp_found': self.timestamp_found,
            'block_found': self.block_found,
            'prices': {token.symbol: str(price) for token, price in self.prices.items()},
            'prices_denom': self.prices_denom.denom,
            'lp_tower_reserves': [reserve.to_data() for reserve in self.lp_tower_reserves],
            'pool_0_lp_balance': self.pool_0_lp_balance.to_data(),
            'direction': self.direction,
            'initial_amount': self.initial_amount.to_data(),
            'msgs': [msg.to_data() for msg in self.msgs],
            'est_final_amount': self.est_final_amount.to_data(),
            'est_fee': self.est_fee.to_data(),
            'est_net_profit_ust': UST.round(self.est_net_profit_ust),
        }


@dataclass
class ArbTx:
    timestamp_sent: float
    tx_hash: str

    def to_data(self) -> dict:
        return {
            'timestamp_sent': self.timestamp_sent,
            'tx_hash': self.tx_hash,
        }


@dataclass
class ArbResult:
    tx_status: TxStatus

    block_send_delay: Optional[int] = None
    timestamp_received: Optional[float] = None
    block_received: Optional[float] = None

    final_amount: Optional[TerraTokenAmount] = None
    gas_use: Optional[int] = None
    gas_cost: Optional[TerraTokenAmount] = None
    tax: Optional[TerraTokenAmount] = None
    net_profit: Optional[TerraTokenAmount] = None

    def to_data(self) -> dict:
        return {
            'tx_status': self.tx_status,
            'block_send_delay': self.block_send_delay,
            'timestamp_received': self.timestamp_received,
            'block_received': self.block_received,
            'final_amount': self.final_amount.to_data() if self.final_amount is not None else None,
            'gas_use': self.gas_use,
            'gas_cost': self.gas_cost.to_data() if self.gas_cost is not None else None,
            'tax': self.tax.to_data() if self.tax is not None else None,
            'net_profit': self.net_profit.to_data() if self.net_profit is not None else None,
        }


@dataclass
class ArbitrageData:
    params: Optional[ArbParams] = None
    tx: Optional[ArbTx] = None
    result: Optional[ArbResult] = None

    @property
    def status(self) -> ExecutionState:
        if self.params is None:
            return ExecutionState.start
        if self.tx is None:
            return ExecutionState.ready_to_broadcast
        if self.result is None:
            return ExecutionState.waiting_confirmation
        return ExecutionState.finished

    def to_data(self) -> dict:
        return {
            'params': None if self.params is None else self.params.to_data(),
            'tx': None if self.tx is None else self.tx.to_data(),
            'result': None if self.result is None else self.result.to_data(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_data())


class LPTowerStrategy:
    def __init__(
        self,
        client: TerraClient,
        pool_0: TerraswapLiquidityPair,
        pool_1: TerraswapLiquidityPair,
    ) -> None:
        self.client = client
        self.pool_0 = pool_0
        self.pool_1 = pool_1
        self.pool_tower = TerraswapLiquidityPair(ADDR_BLUNA_LUNA_UST_TOWER_POOL, client)
        self.arbitrage_data = ArbitrageData()

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(client={self.client}, '
            f'pool_tower={self.pool_tower}, status={self.arbitrage_data.status})'
        )

    def run(self, block: int, mempool: dict = None):
        if self.arbitrage_data.status == ExecutionState.waiting_confirmation:
            log.debug('Looking for tx confirmation(s)')
            try:
                self.arbitrage_data.result = self._confirm_tx(block)
            except IsBusy:
                return
            else:
                log.info('Arbitrage executed:')
                log.info(self.arbitrage_data.to_json())
                self.arbitrage_data = ArbitrageData()
        log.debug('Generating execution configuration')
        try:
            self.arbitrage_data.params = params = self._get_arbitrage_params(block, mempool)
        except UnprofitableArbitrage as e:
            log.info(e)
            return
        log.debug('Broadcasting transaction')
        try:
            self.arbitrage_data.tx = self._broadcast_tx(params, block)
        except BlockchainNewState as e:
            log.warning(e)
            return
        else:
            log.info('Arbitrage broadcasted:')
            log.info(self.arbitrage_data.to_json())

    def _confirm_tx(self, block: int) -> ArbResult:
        raise NotImplementedError

    def _get_arbitrage_params(self, block: int, mempool: dict = None) -> ArbParams:
        if mempool:
            raise NotImplementedError
        prices = self._get_prices()
        balance_ratio, direction = self._get_pool_balance_ratio(
            prices[self.pool_0.lp_token],
            prices[self.pool_1.lp_token],
        )

        lp_ust_price = prices[self.pool_0.lp_token] * self.client.get_exchange_rate(LUNA, UST)
        pool_0_lp_balance = self.pool_0.lp_token.get_balance(self.client)
        initial_amount = self._get_optimal_argitrage_amount(
            lp_ust_price, direction, pool_0_lp_balance, balance_ratio
        )
        final_amount, msgs = self._get_amount_out_and_msgs(initial_amount, direction)
        fee = self.client.estimate_fee(msgs)
        gas_cost = TerraTokenAmount.from_coin(*fee.amount.to_list())
        net_profit_ust = (final_amount - initial_amount).amount * lp_ust_price - gas_cost.amount
        if net_profit_ust < MIN_PROFIT_UST:
            raise UnprofitableArbitrage(
                f'Low profitability: USD {net_profit_ust:.2f}, {balance_ratio=:0.3%}')
        margin = net_profit_ust / (initial_amount.amount * lp_ust_price)
        if margin < MIN_NET_PROFIT_MARGIN:
            raise UnprofitableArbitrage(
                f'Low profitability margin: USD {margin:.3%}, {balance_ratio=:0.3%}')

        return ArbParams(
            timestamp_found=time.time(),
            block_found=block,
            prices=prices,
            prices_denom=LUNA,
            lp_tower_reserves=self.pool_tower.reserves,
            pool_0_lp_balance=pool_0_lp_balance,
            direction=direction,
            initial_amount=initial_amount,
            msgs=msgs,
            est_final_amount=final_amount,
            est_fee=fee,
            est_net_profit_ust=net_profit_ust,
        )

    def _get_prices(self) -> dict[TerraToken, Decimal]:
        bluna_price = self.pool_0.reserves[1].amount / self.pool_0.reserves[0].amount
        ust_price = self.pool_1.reserves[1].amount / self.pool_1.reserves[0].amount
        return {
            self.pool_0.lp_token: self.pool_0.get_price(LUNA),
            self.pool_1.lp_token: self.pool_1.get_price(LUNA),
            self.pool_0.tokens[0]: bluna_price,
            self.pool_1.tokens[0]: ust_price,
        }

    def _get_pool_balance_ratio(
        self,
        pool_0_lp_price: Decimal,
        pool_1_lp_price: Decimal,
    ) -> tuple[Decimal, Direction]:
        pool_0_reserve_value = self.pool_tower.reserves[0].amount * pool_0_lp_price
        pool_1_reserve_value = self.pool_tower.reserves[1].amount * pool_1_lp_price
        balance_ratio = pool_0_reserve_value / pool_1_reserve_value - 1
        if balance_ratio > 0:
            return balance_ratio, Direction.remove_liquidity_first
        return balance_ratio, Direction.swap_first

    def _get_optimal_argitrage_amount(
        self,
        lp_ust_price: Decimal,
        direction: Direction,
        pool_0_lp_balance: TerraTokenAmount,
        balance_ratio: Decimal,
    ) -> TerraTokenAmount:
        initial_lp_amount = TerraTokenAmount(
            self.pool_0.lp_token, MIN_START_AMOUNT.amount / lp_ust_price)
        profit = self._get_gross_profit(initial_lp_amount, direction)
        if profit.amount * lp_ust_price < 0:
            raise UnprofitableArbitrage(f'No profitability, {balance_ratio=:0.3%}')
        func = partial(self._get_gross_profit_dec, direction=direction)
        lp_amount, _ = utils.optimization.optimize(
            func,
            x0=initial_lp_amount.amount,
            dx=initial_lp_amount.dx,
            tol=OPTIMIZATION_TOLERANCE.amount / lp_ust_price,
        )
        amount = TerraTokenAmount(self.pool_0.lp_token, lp_amount)
        return min(amount, pool_0_lp_balance)

    def _get_gross_profit(
        self,
        initial_lp_amount: TerraTokenAmount,
        direction: Direction,
    ) -> TerraTokenAmount:
        amount_out = self._get_amount_out_and_msgs(initial_lp_amount, direction)[0]
        return amount_out - initial_lp_amount

    def _get_gross_profit_dec(
        self,
        amount: Decimal,
        direction: Direction,
    ) -> Decimal:
        token_amount = TerraTokenAmount(self.pool_0.lp_token, amount)
        return self._get_gross_profit(token_amount, direction).amount

    def _get_amount_out_and_msgs(
        self,
        initial_lp_amount: TerraTokenAmount,
        direction: Direction,
    ) -> tuple[TerraTokenAmount, list[MsgExecuteContract]]:
        if direction == Direction.remove_liquidity_first:
            luna_amount, msgs_remove_single_side = self.pool_0.op_remove_single_side(
                self.client.address, initial_lp_amount, LUNA, MAX_SLIPPAGE
            )
            lp_amount, msgs_add_single_side = self.pool_1.op_add_single_side(
                self.client.address, luna_amount, MAX_SLIPPAGE
            )
            final_lp_amount, msgs_tower_swap = self.pool_tower.op_swap(
                self.client.address, lp_amount, MAX_SLIPPAGE
            )
            msgs = msgs_remove_single_side + msgs_add_single_side + msgs_tower_swap
        else:
            lp_amount, msgs_tower_swap = self.pool_tower.op_swap(
                self.client.address, initial_lp_amount, MAX_SLIPPAGE
            )
            luna_amount, msgs_remove_single_side = self.pool_1.op_remove_single_side(
                self.client.address, lp_amount, LUNA, MAX_SLIPPAGE
            )
            final_lp_amount, msgs_add_single_side = self.pool_0.op_add_single_side(
                self.client.address, luna_amount, MAX_SLIPPAGE
            )
            msgs = msgs_tower_swap + msgs_remove_single_side + msgs_add_single_side
        return final_lp_amount, msgs

    def _broadcast_tx(self, execution_config: ArbParams, block: int) -> ArbTx:
        tx_hash = self.client.execute_msgs(execution_config.msgs, fee=execution_config.est_fee)
        if (latest_block := self.client.get_latest_block()) != block:
            raise BlockchainNewState(f'{latest_block=} different from {block=}')
        return ArbTx(
            timestamp_sent=time.time(),
            tx_hash=tx_hash,
        )


def run():
    client = TerraClient()
    pool_0 = TerraswapLiquidityPair(ADDR_BLUNA_LUNA_POOL, client)
    pool_1 = TerraswapLiquidityPair(ADDR_UST_LUNA_POOL, client)
    strategy = LPTowerStrategy(client, pool_0, pool_1)
    for block in client.wait_next_block():
        strategy.run(block)
        utils.cache.clear_caches(utils.cache.CacheGroup.TERRA)
