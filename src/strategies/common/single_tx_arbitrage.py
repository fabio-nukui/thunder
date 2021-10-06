from __future__ import annotations

import logging
from abc import ABC, abstractclassmethod, abstractmethod
from enum import Enum
from typing import Generic, Optional, TypeVar

from common import BlockchainClient
from exceptions import BlockchainNewState, IsBusy, TxError, UnprofitableArbitrage

log = logging.getLogger(__name__)


class ExecutionState(str, Enum):
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

    @abstractclassmethod
    def to_data(self) -> dict:
        ...


class BaseArbTx(ABC):
    timestamp_sent: float
    tx_hash: str

    def to_data(self) -> dict:
        ...


class BaseArbResult(ABC):
    tx_status: TxStatus

    def to_data(self) -> dict:
        ...


class ArbitrageData:
    params: Optional[BaseArbParams] = None
    tx: Optional[BaseArbTx] = None
    result: Optional[BaseArbResult] = None

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
            "params": None if self.params is None else self.params.to_data(),
            "tx": None if self.tx is None else self.tx.to_data(),
            "result": None if self.result is None else self.result.to_data(),
        }


_BlockchainClientT = TypeVar("_BlockchainClientT", bound=BlockchainClient)


class SingleTxArbitrage(Generic[_BlockchainClientT], ABC):
    def __init__(self, client: _BlockchainClientT):
        self.client = client
        self.arbitrage_data = ArbitrageData()
        log.info(f"Initialized {self} at block={self.client.block}")

    def run(self, block: int, mempool: dict = None):
        if self.arbitrage_data.status == ExecutionState.waiting_confirmation:
            log.debug("Looking for tx confirmation(s)")
            try:
                self.arbitrage_data.result = self._confirm_tx(block)
            except IsBusy:
                return
            else:
                log.info("Arbitrage executed", extra={"data": self.arbitrage_data.to_data()})
                self.arbitrage_data = ArbitrageData()
        log.debug("Generating execution configuration")
        try:
            self.arbitrage_data.params = params = self._get_arbitrage_params(block, mempool)
        except (UnprofitableArbitrage, TxError) as e:
            log.debug(e)
            return
        log.debug("Broadcasting transaction")
        try:
            self.arbitrage_data.tx = self._broadcast_tx(params, block)
        except BlockchainNewState as e:
            log.warning(e)
            return
        else:
            log.info("Arbitrage broadcasted", extra={"data": self.arbitrage_data.to_data()})

    @abstractmethod
    def _confirm_tx(self, block: int) -> BaseArbResult:
        ...

    @abstractmethod
    def _get_arbitrage_params(self, block: int, mempool: dict = None) -> BaseArbParams:
        ...

    @abstractmethod
    def _broadcast_tx(self, execution_config: BaseArbParams, block: int) -> BaseArbTx:
        ...
