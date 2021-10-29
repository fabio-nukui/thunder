from __future__ import annotations

from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Generic, Optional, TypeVar

import utils
from common import BlockchainClient, TokenAmount
from exceptions import (
    BlockchainNewState,
    FeeEstimationError,
    IsBusy,
    OptimizationError,
    TxAlreadyBroadcasted,
    UnprofitableArbitrage,
)

_BlockchainClientT = TypeVar("_BlockchainClientT", bound=BlockchainClient)


class State(str, Enum):
    ready_to_generate_parameters = "ready_to_generate_parameters"
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

    n_repeat: int

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


def _format_status_logs(results: list[ArbResult]) -> str:
    counts = Counter(res.tx_status for res in results)
    return ",".join(f"{status}({counts[status]}x)" for status in TxStatus if counts[status])


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
    txs: Optional[list[ArbTx]] = None
    results: Optional[list[ArbResult]] = None

    def to_data(self) -> dict:
        return {
            "params": None if self.params is None else self.params.to_data(),
            "txs": None if self.txs is None else [tx.to_data() for tx in self.txs],
            "results": None if self.results is None else [res.to_data() for res in self.results],
        }

    def reset(self):
        self.params = None
        self.txs = None
        self.results = None


class RepeatedTxArbitrage(Generic[_BlockchainClientT], ABC):
    def __init__(self, client: _BlockchainClientT, broadcast_kwargs: dict = None):
        self.client = client
        self.broadcast_kwargs = broadcast_kwargs or {}
        self.last_run_height = 0

        self.data = ArbitrageData()
        self.log = utils.logger.ReformatedLogger(__name__, formater=self._log_formatter)
        self.log.info("Initialized")

    def _log_formatter(self, msg: Any) -> str:
        return f"{self} height={self.client.height}: {msg}"

    @abstractmethod
    def _reset_mempool_params(self):
        ...

    @property
    def state(self) -> State:
        if self.data.params is None:
            return State.ready_to_generate_parameters
        if self.data.txs is None:
            return State.ready_to_broadcast
        if self.data.results is None:
            return State.waiting_confirmation
        return State.finished

    async def run(self, height: int, filtered_mempool: dict[Any, list[list[dict]]] = None):
        if height > self.last_run_height:
            self._reset_mempool_params()
        try:
            if self.state == State.waiting_confirmation:
                if self.last_run_height >= height:
                    return
                self.log.debug("Looking for tx confirmation(s)")
                try:
                    params: BaseArbParams = self.data.params  # type: ignore
                    txs: list[ArbTx] = self.data.txs  # type: ignore
                    self.data.results = await self._confirm_txs(height, params, txs)
                    profit = sum(res.net_profit_usd or 0 for res in self.data.results)
                    self.log.info(
                        f"Arbitrage {_format_status_logs(self.data.results)}, "
                        f"net_profit_usd={profit:.2f}",
                        extra={"data": self.data.to_data()},
                    )
                    if any(
                        res.tx_err_log and "out of gas" in res.tx_err_log
                        for res in self.data.results
                    ):
                        self.log.warning("Out of gas")
                    self.data.reset()
                except IsBusy:
                    return
            if self.state == State.ready_to_generate_parameters:
                self.log.debug("Generating arbitrage parameters")
                try:
                    self.data.params = await self._get_arbitrage_params(height, filtered_mempool)
                except (
                    UnprofitableArbitrage,
                    FeeEstimationError,
                    OptimizationError,
                    TxAlreadyBroadcasted,
                ) as e:
                    self.log.debug(e)
                    return
            if self.state == State.ready_to_broadcast:
                n_txs = self.data.params.n_repeat  # type: ignore
                self.log.info(f"Broadcasting {n_txs} transaction(s)")
                try:
                    arb_params: BaseArbParams = self.data.params  # type: ignore
                    self.data.txs = await self._broadcast_txs(
                        arb_params, height, **self.broadcast_kwargs
                    )
                    self.log.debug("Arbitrage broadcasted", extra={"data": self.data.to_data()})
                except TxAlreadyBroadcasted as e:
                    self.log.debug(e)
                    self.data.reset()
                except BlockchainNewState as e:
                    self.log.warning(e)
                    self.data.reset()
                return
        finally:
            self.last_run_height = height

    @abstractmethod
    async def _get_arbitrage_params(
        self,
        height: int,
        filtered_mempool: dict[Any, list[list[dict]]] = None,
    ) -> BaseArbParams:
        ...

    @abstractmethod
    async def _broadcast_txs(self, arb_params: BaseArbParams, height: int, **kwargs) -> list[ArbTx]:
        ...

    @abstractmethod
    async def _confirm_txs(
        self,
        height: int,
        params: BaseArbParams,
        txs: list[ArbTx],
    ) -> list[ArbResult]:
        ...
