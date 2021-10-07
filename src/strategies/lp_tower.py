from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from functools import partial

from terra_sdk.core.auth import StdFee, TxLog
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
from exceptions import BlockchainNewState, IsBusy, TxError, UnprofitableArbitrage

from .common.single_tx_arbitrage import ArbResult, ArbTx, BaseArbParams, SingleTxArbitrage, TxStatus

log = logging.getLogger(__name__)

MIN_PROFIT_UST = UST.to_amount(2)
MIN_START_AMOUNT = UST.to_amount(10)
OPTIMIZATION_TOLERANCE = UST.to_amount("0.01")
MIN_CONFIRMATIONS = 1
MAX_BLOCKS_WAIT_RECEIPT = 10
MAX_SLIPPAGE = Decimal("0.001")
SWAP_FIRST_LAST_MSG_UST_TOL = Decimal("0.5")


class Direction(str, Enum):
    remove_liquidity_first = "remove_liquidity_first"
    swap_first = "swap_first"


@dataclass
class ArbParams(BaseArbParams):
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
            "est_net_profit_ust": float(self.est_net_profit_ust),
        }


class LPTowerStrategy(SingleTxArbitrage[TerraClient]):
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

        self._amount_luna_swap_first_last_msg_tol = Decimal(0)
        self._flag_last_msg_tol = False

        super().__init__(client)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(client={self.client}, "
            f"pool_tower={self.pool_tower}, state={self.state})"
        )

    def _get_arbitrage_params(self, block: int, mempool: dict = None) -> ArbParams:
        if mempool:
            raise NotImplementedError
        prices = self._get_prices()
        balance_ratio, direction = self._get_pool_balance_ratio(
            prices[self.pool_0.lp_token],
            prices[self.pool_1.lp_token],
        )

        lp_ust_price = prices[self.pool_0.lp_token] * self.client.oracle.get_exchange_rate(
            LUNA, UST
        )
        pool_0_lp_balance = self.pool_0.lp_token.get_balance(self.client)
        self._amount_luna_swap_first_last_msg_tol = SWAP_FIRST_LAST_MSG_UST_TOL * prices[UST]
        initial_amount = self._get_optimal_argitrage_amount(
            lp_ust_price, direction, pool_0_lp_balance, balance_ratio
        )
        final_amount, msgs = self._get_amount_out_and_msgs(initial_amount, direction)
        try:
            fee = self.client.tx.estimate_fee(msgs)
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
        net_profit_ust = (final_amount - initial_amount).amount * lp_ust_price - gas_cost.amount
        if net_profit_ust < MIN_PROFIT_UST:
            raise UnprofitableArbitrage(
                f"Low profitability: USD {net_profit_ust:.2f}, {balance_ratio=:0.3%}"
            )

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
        initial_lp_amount = self.pool_0.lp_token.to_amount(MIN_START_AMOUNT.amount / lp_ust_price)
        profit = self._get_gross_profit(initial_lp_amount, direction)
        if profit.amount * lp_ust_price < 0:
            raise UnprofitableArbitrage(f"No profitability, {balance_ratio=:0.3%}")
        func = partial(self._get_gross_profit_dec, direction=direction)
        with self._activate_last_msg_tol():
            lp_amount, _ = utils.optimization.optimize_bissection(
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

    @contextmanager
    def _activate_last_msg_tol(self):
        flag = self._flag_last_msg_tol
        try:
            self._flag_last_msg_tol = True
            yield
        finally:
            self._flag_last_msg_tol = flag

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
        token_amount = self.pool_0.lp_token.to_amount(amount)
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
            if self._flag_last_msg_tol:
                luna_amount.amount = luna_amount.amount - self._amount_luna_swap_first_last_msg_tol
            final_lp_amount, msgs_add_single_side = self.pool_0.op_add_single_side(
                self.client.address, luna_amount, MAX_SLIPPAGE
            )
            msgs = msgs_tower_swap + msgs_remove_single_side + msgs_add_single_side
        return final_lp_amount, msgs

    def _broadcast_tx(self, execution_config: ArbParams, block: int) -> ArbTx:
        if (latest_block := self.client.get_latest_block()) != block:
            raise BlockchainNewState(f"{latest_block=} different from {block=}")
        res = self.client.tx.execute_msgs_async(execution_config.msgs, fee=execution_config.est_fee)
        return ArbTx(timestamp_sent=time.time(), tx_hash=res.txhash)

    def _confirm_tx(self, block: int) -> ArbResult:
        assert self.data.params is not None
        assert self.data.tx is not None
        tx_inclusion_delay = block - self.data.params.block_found
        try:
            info = self.client.lcd.tx.tx_info(self.data.tx.tx_hash)
        except LCDResponseError as e:
            if e.response.status == 404:
                if tx_inclusion_delay >= MAX_BLOCKS_WAIT_RECEIPT:
                    return ArbResult(TxStatus.not_found)
                raise IsBusy
            raise
        log.debug(info.to_data())
        if block - info.height < MIN_CONFIRMATIONS:
            raise IsBusy
        gas_cost = TerraTokenAmount.from_coin(*info.tx.fee.amount)
        if info.logs is None:
            status = TxStatus.failed
            tx_err_log = info.rawlog
            final_amount = None
            net_profit_ust = -gas_cost.amount
        else:
            status = TxStatus.succeeded
            tx_err_log = None
            final_amount, net_profit_ust = self._extract_returns_from_logs(info.logs)
        return ArbResult(
            tx_status=status,
            tx_err_log=tx_err_log,
            gas_use=info.gas_used,
            gas_cost=gas_cost,
            tx_inclusion_delay=tx_inclusion_delay,
            timestamp_received=datetime.fromisoformat(info.timestamp[:-1]).timestamp(),
            block_received=info.height,
            final_amount=final_amount,
            net_profit_usd=net_profit_ust,
        )

    def _extract_returns_from_logs(self, logs: list[TxLog]) -> tuple[TerraTokenAmount, Decimal]:
        tx_events = self.client.extract_log_events(logs)
        logs_from_contract = self.client.parse_from_contract_events(tx_events)
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

        pool_0_lp_price_luna = self._get_prices()[self.pool_0.lp_token]
        pool_0_lp_price_ust = pool_0_lp_price_luna * self.client.oracle.get_exchange_rate(LUNA, UST)
        increase_tokens = final_amount - first_amount

        return final_amount, round(increase_tokens.amount * pool_0_lp_price_ust, 18)


def run():
    client = TerraClient()
    addresses = terraswap.get_addresses(client.chain_id)["pools"]
    pool_0 = terraswap.LiquidityPair(addresses["bluna_luna"], client)
    pool_1 = terraswap.LiquidityPair(addresses["ust_luna"], client)
    pool_tower = terraswap.LiquidityPair(addresses["bluna_luna_ust_luna"], client)
    strategy = LPTowerStrategy(client, pool_0, pool_1, pool_tower)
    for block in client.wait_next_block():
        strategy.run(block)
        utils.cache.clear_caches(utils.cache.CacheGroup.TERRA)
