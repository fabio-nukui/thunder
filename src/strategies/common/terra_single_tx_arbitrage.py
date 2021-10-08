import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal

from terra_sdk.core.auth import StdFee, TxLog
from terra_sdk.core.wasm import MsgExecuteContract
from terra_sdk.exceptions import LCDResponseError

from chains.terra import TerraClient, TerraTokenAmount
from exceptions import BlockchainNewState, IsBusy

from .single_tx_arbitrage import ArbResult, ArbTx, BaseArbParams, SingleTxArbitrage, TxStatus

log = logging.getLogger(__name__)

MIN_CONFIRMATIONS = 1
MAX_BLOCKS_WAIT_RECEIPT = 10


class TerraArbParams(BaseArbParams):
    timestamp_found: float
    block_found: int

    msgs: list[MsgExecuteContract]
    est_fee: StdFee


class TerraSingleTxArbitrage(SingleTxArbitrage[TerraClient], ABC):
    def _broadcast_tx(self, execution_config: TerraArbParams, block: int) -> ArbTx:
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

    @abstractmethod
    def _extract_returns_from_logs(self, logs: list[TxLog]) -> tuple[TerraTokenAmount, Decimal]:
        ...