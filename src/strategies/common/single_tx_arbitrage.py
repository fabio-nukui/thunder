from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Generic, Optional, TypeVar

from common import BlockchainClient, TokenAmount
from exceptions import BlockchainNewState, IsBusy, TxError, UnprofitableArbitrage

log = logging.getLogger(__name__)


_BlockchainClientT = TypeVar("_BlockchainClientT", bound=BlockchainClient)


class State(str, Enum):
    start = "start"
    ready_to_broadcast = "ready_to_broadcast"
    waiting_confirmation = "waiting_confirmation"
    finished = "finished"


class TxStatus(str, Enum):
    succeeded = "succeeded"
    failed = "failed"
    not_found = "not found"


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


class SingleTxArbitrage(Generic[_BlockchainClientT], ABC):
    def __init__(self, client: _BlockchainClientT):
        self.client = client
        self.data = ArbitrageData()
        log.info(f"Initialized {self} at height={self.client.height}")
        self.last_height_run = 0

    @abstractmethod
    def _reset_mempool_params(self):
        ...

    @property
    def state(self) -> State:
        if self.data.params is None:
            return State.start
        if self.data.tx is None:
            return State.ready_to_broadcast
        if self.data.result is None:
            return State.waiting_confirmation
        return State.finished

    async def run(self, height: int, filtered_mempool: dict[Any, list[list[dict]]] = None):
        if height > self.last_height_run:
            self._reset_mempool_params()
        try:
            if self.state == State.waiting_confirmation:
                if self.last_height_run >= height:
                    return
                log.debug(f"{self} ({height=}) Looking for tx confirmation(s)")
                try:
                    self.data.result = await self._confirm_tx(height)
                    log.info(
                        f"Arbitrage {self.data.result.tx_status}",
                        extra={"data": self.data.to_data()},
                    )
                    self.data.reset()
                except IsBusy:
                    return
            if self.state == State.start:
                log.debug(f"{self} ({height=}) Generating arbitrage parameters")
                try:
                    self.data.params = await self._get_arbitrage_params(height, filtered_mempool)
                except (UnprofitableArbitrage, TxError) as e:
                    log.debug(e)
                    return
            if self.state == State.ready_to_broadcast:
                log.info(f"{self} ({height=}) Broadcasting transaction")
                try:
                    arb_params: BaseArbParams = self.data.params  # type: ignore
                    self.data.tx = await self._broadcast_tx(arb_params, height)
                    log.debug("Arbitrage broadcasted", extra={"data": self.data.to_data()})
                except BlockchainNewState as e:
                    log.warning(e)
                    self.data.reset()
                return
        finally:
            self.last_height_run = height

    @abstractmethod
    async def _get_arbitrage_params(
        self,
        height: int,
        filtered_mempool: dict[Any, list[list[dict]]] = None,
    ) -> BaseArbParams:
        ...

    @abstractmethod
    async def _broadcast_tx(self, arb_params: BaseArbParams, height: int) -> ArbTx:
        ...

    @abstractmethod
    async def _confirm_tx(self, height: int) -> ArbResult:
        ...
