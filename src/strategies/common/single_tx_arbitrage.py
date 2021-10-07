from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Generic, Optional, TypeVar

from common import BlockchainClient
from common.token import TokenAmount
from exceptions import BlockchainNewState, IsBusy, TxError, UnprofitableArbitrage

log = logging.getLogger(__name__)


class State(str, Enum):
    start = "start"
    ready_to_broadcast = "ready_to_broadcast"
    waiting_confirmation = "waiting_confirmation"
    finished = "finished"


class TxStatus(str, Enum):
    succeeded = "succeeded"
    failed = "failed"
    not_found = "not_found"


class BaseArbParams(ABC):
    timestamp_found: float
    block_found: int

    @abstractmethod
    def to_data(self) -> dict:
        ...


@dataclass
class ArbTx:
    timestamp_sent: float
    tx_hash: str

    def to_data(self) -> dict:
        return {
            "timestamp_sent": self.timestamp_sent,
            "tx_hash": self.tx_hash,
        }


@dataclass
class ArbResult:
    tx_status: TxStatus
    tx_err_log: Optional[str] = None
    gas_use: Optional[int] = None
    gas_cost: Optional[TokenAmount] = None

    tx_inclusion_delay: Optional[int] = None
    timestamp_received: Optional[float] = None
    block_received: Optional[float] = None

    final_amount: Optional[TokenAmount] = None
    net_profit_usd: Optional[Decimal] = None

    def to_data(self) -> dict:
        return {
            "tx_status": self.tx_status,
            "tx_err_log": self.tx_err_log,
            "gas_use": self.gas_use,
            "gas_cost": None if self.gas_cost is None else self.gas_cost.to_data(),
            "tx_inclusion_delay": self.tx_inclusion_delay,
            "timestamp_received": self.timestamp_received,
            "block_received": self.block_received,
            "final_amount": None if self.final_amount is None else self.final_amount.to_data(),
            "net_profit_usd": None if self.net_profit_usd is None else float(self.net_profit_usd),
        }


class ArbitrageData:
    params: Optional[BaseArbParams] = None
    tx: Optional[ArbTx] = None
    result: Optional[ArbResult] = None

    def to_data(self) -> dict:
        return {
            "params": None if self.params is None else self.params.to_data(),
            "tx": None if self.tx is None else self.tx.to_data(),
            "result": None if self.result is None else self.result.to_data(),
        }

    def reset(self):
        self.params = None
        self.tx = None
        self.result = None


_BlockchainClientT = TypeVar("_BlockchainClientT", bound=BlockchainClient)


class SingleTxArbitrage(Generic[_BlockchainClientT], ABC):
    def __init__(self, client: _BlockchainClientT):
        self.client = client
        self.data = ArbitrageData()
        log.info(f"Initialized {self} at block={self.client.block}")

    @property
    def state(self) -> State:
        if self.data.params is None:
            return State.start
        if self.data.tx is None:
            return State.ready_to_broadcast
        if self.data.result is None:
            return State.waiting_confirmation
        return State.finished

    def run(self, block: int, mempool: dict = None):
        if self.state == State.start:
            log.debug("Generating execution configuration")
            try:
                self.data.params = self._get_arbitrage_params(block, mempool)
            except (UnprofitableArbitrage, TxError) as e:
                log.debug(e)
                return
        if self.state == State.ready_to_broadcast:
            log.info("Broadcasting transaction")
            try:
                self.data.tx = self._broadcast_tx(self.data.params, block)  # type: ignore
            except BlockchainNewState as e:
                log.warning(e)
                return
            else:
                log.debug("Arbitrage broadcasted", extra={"data": self.data.to_data()})
                return
        if self.state == State.waiting_confirmation:
            log.debug("Looking for tx confirmation(s)")
            try:
                self.data.result = self._confirm_tx(block)
            except IsBusy:
                return
            else:
                log.info("Arbitrage executed", extra={"data": self.data.to_data()})
                self.data.reset()

    @abstractmethod
    def _get_arbitrage_params(self, block: int, mempool: dict = None) -> BaseArbParams:
        ...

    @abstractmethod
    def _broadcast_tx(self, execution_config: BaseArbParams, block: int) -> ArbTx:
        ...

    @abstractmethod
    def _confirm_tx(self, block: int) -> ArbResult:
        ...
