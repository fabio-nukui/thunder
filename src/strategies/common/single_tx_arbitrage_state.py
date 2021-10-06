from abc import ABC, abstractclassmethod
from enum import Enum
from typing import Optional


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
