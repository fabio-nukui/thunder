import asyncio
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import Any, Awaitable, Generic, Iterable, Mapping, Sequence, TypeVar

from cosmos_sdk.core.auth import TxInfo
from cosmos_sdk.core.fee import Fee
from cosmos_sdk.core.wasm import MsgExecuteContract
from cosmos_sdk.exceptions import LCDResponseError

import utils
from chains.cosmos.client import CosmosClient
from chains.cosmos.token import CosmosTokenAmount
from chains.cosmos.tx_filter import Filter
from exceptions import BlockchainNewState, IsBusy

from ..repeated_tx_arbitrage import (
    ArbResult,
    ArbTx,
    BaseArbParams,
    RepeatedTxArbitrage,
    TxStatus,
)

log = logging.getLogger(__name__)
_CosmosClientT = TypeVar("_CosmosClientT", bound=CosmosClient)

MAX_BLOCK_BROADCAST_DELAY = 1
MIN_CONFIRMATIONS = 1
MAX_BLOCKS_WAIT_RECEIPT = 4


class CosmosArbParams(BaseArbParams):
    timestamp_found: float
    block_found: int

    n_repeat: int
    msgs: list[MsgExecuteContract]
    est_fee: Fee


class CosmosRepeatedTxArbitrage(
    Generic[_CosmosClientT], RepeatedTxArbitrage[_CosmosClientT], ABC
):
    def __init__(
        self,
        *args,
        filter_keys: Iterable,
        fee_denom: str = None,
        cls_amount: type[CosmosTokenAmount] = CosmosTokenAmount,
        **kwargs,
    ):
        self.filter_keys = filter_keys
        self.fee_denom = fee_denom
        self.cls_amount = cls_amount
        broadcast_kwargs = kwargs.get("broadcast_kwargs", {})
        kwargs["broadcast_kwargs"] = broadcast_kwargs | {"fee_denom": fee_denom}
        super().__init__(*args, **kwargs)

    async def _broadcast_txs(  # type: ignore[override]
        self,
        arb_params: CosmosArbParams,
        height: int,
        fee_denom: str = None,
    ) -> list[ArbTx]:
        latest_height = await self.client.get_latest_height()
        if latest_height - height > MAX_BLOCK_BROADCAST_DELAY:
            raise BlockchainNewState(f"{latest_height=} different from {height=}")
        results = await self.client.tx.execute_multi_msgs(
            arb_params.msgs, arb_params.n_repeat, fee=arb_params.est_fee, fee_denom=fee_denom
        )
        return [
            ArbTx(timestamp_sent=timestamp, tx_hash=res.txhash) for timestamp, res in results
        ]

    async def _confirm_txs(  # type: ignore[override]
        self,
        height: int,
        params: CosmosArbParams,
        txs: list[ArbTx],
    ) -> list[ArbResult]:
        results = await asyncio.gather(
            *(self._confirm_single_tx(height, params, tx.tx_hash) for tx in txs)
        )
        return list(results)

    async def _confirm_single_tx(
        self,
        height: int,
        params: CosmosArbParams,
        tx_hash: str,
    ) -> ArbResult:
        tx_inclusion_delay = height - params.block_found
        try:
            info = await self.client.lcd.tx.tx_info(tx_hash)
        except LCDResponseError as e:
            status_code = e.response.status
            if status_code == 404 or status_code == 400 and "not found" in e.message:
                if tx_inclusion_delay >= MAX_BLOCKS_WAIT_RECEIPT:
                    return ArbResult(TxStatus.not_found)
                raise IsBusy
            raise
        if height - info.height < MIN_CONFIRMATIONS:
            raise IsBusy
        gas_cost = self.cls_amount.from_coin(*info.tx.auth_info.fee.amount)
        if info.logs is None:
            status = TxStatus.failed
            tx_err_log = info.rawlog
            final_amount = None
            net_profit_ust = -gas_cost.amount
        else:
            status = TxStatus.succeeded
            tx_err_log = None
            final_amount, net_profit_ust = await self._extract_returns_from_info(info)
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
    async def _extract_returns_from_info(
        self,
        logs: TxInfo,
    ) -> tuple[CosmosTokenAmount, Decimal]:
        ...


async def run_strategy(
    client: CosmosClient,
    arb_routes: Sequence[CosmosRepeatedTxArbitrage],
    mempool_filters: Mapping[Any, Filter],
    cache_group: utils.cache.CacheGroup,
    max_n_blocks: int = None,
    log_time: bool = True,
    verbose_decode_warnings: bool = True,
):
    log.info(f"Running strategy with {len(arb_routes)=} and {len(mempool_filters)=}")
    start_height = client.height
    async for height, mempool in client.mempool.iter_height_mempool(
        mempool_filters, verbose_decode_warnings
    ):
        if log_time:
            start = time.perf_counter()
        if is_new_block := any(height > arb_route.last_run_height for arb_route in arb_routes):
            log.debug(f"New block: {height=}")
            utils.cache.clear_caches(cache_group)
            asyncio.create_task(client.update_active_broadcaster())
        if mempool:
            log.debug(f"New mempool txs: n_txs={len(mempool)}")
        tasks: list[Awaitable] = []
        for arb_route in arb_routes:
            mempool_route = {
                k: list_msgs for k, list_msgs in mempool.items() if k in arb_route.filter_keys
            }
            if height > arb_route.last_run_height or mempool_route:
                tasks.append(arb_route.run(height, mempool_route))
        await asyncio.gather(*tasks)
        if max_n_blocks is not None and (n_blocks := height - start_height) >= max_n_blocks:
            break
        if log_time:
            total_time_ms = (time.perf_counter() - start) * 1000  # type: ignore
            stats = f"{total_time_ms:.1f}ms; {len(tasks)=}"
            if is_new_block:
                log.debug(f"Processed block: {height=} in {stats}")
            else:
                log.debug(f"Processed mempool txs: n_txs={len(mempool)} in {stats}")
    log.info(f"Stopped execution after {n_blocks=}")
