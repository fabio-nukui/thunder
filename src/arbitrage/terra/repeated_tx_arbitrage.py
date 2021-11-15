import asyncio
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import Any, Awaitable, Iterable, Mapping, Sequence

from terra_sdk.core.auth import TxInfo
from terra_sdk.core.fee import Fee
from terra_sdk.core.wasm import MsgExecuteContract
from terra_sdk.exceptions import LCDResponseError

import utils
from chains.terra import TerraClient, TerraTokenAmount
from chains.terra.tx_filter import Filter
from exceptions import BlockchainNewState, IsBusy

from ..repeated_tx_arbitrage import (
    ArbResult,
    ArbTx,
    BaseArbParams,
    RepeatedTxArbitrage,
    State,
    TxStatus,
)

log = logging.getLogger(__name__)

MIN_CONFIRMATIONS = 1
MAX_BLOCKS_WAIT_RECEIPT = 4


class TerraArbParams(BaseArbParams):
    timestamp_found: float
    block_found: int

    n_repeat: int
    msgs: list[MsgExecuteContract]
    est_fee: Fee


class TerraRepeatedTxArbitrage(RepeatedTxArbitrage[TerraClient], ABC):
    def __init__(self, *args, filter_keys: Iterable, fee_denom: str = None, **kwargs):
        self.filter_keys = filter_keys
        self.fee_denom = fee_denom
        kwargs = kwargs.get("broadcast_kwargs", {})
        kwargs["broadcast_kwargs"] = kwargs | {"fee_denom": fee_denom}
        super().__init__(*args, **kwargs)

    async def _broadcast_txs(  # type: ignore[override]
        self,
        arb_params: TerraArbParams,
        height: int,
        fee_denom: str = None,
    ) -> list[ArbTx]:
        if (latest_height := await self.client.get_latest_height()) != height:
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
        params: TerraArbParams,
        txs: list[ArbTx],
    ) -> list[ArbResult]:
        results = await asyncio.gather(
            *(self._confirm_single_tx(height, params, tx.tx_hash) for tx in txs)
        )
        return list(results)

    async def _confirm_single_tx(
        self,
        height: int,
        params: TerraArbParams,
        tx_hash: str,
    ) -> ArbResult:
        tx_inclusion_delay = height - params.block_found
        try:
            info = await self.client.lcd.tx.tx_info(tx_hash)
        except LCDResponseError as e:
            if e.response.status == 404:
                if tx_inclusion_delay >= MAX_BLOCKS_WAIT_RECEIPT:
                    return ArbResult(TxStatus.not_found)
                raise IsBusy
            raise
        if height - info.height < MIN_CONFIRMATIONS:
            raise IsBusy
        gas_cost = TerraTokenAmount.from_coin(*info.tx.auth_info.fee.amount)
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
    ) -> tuple[TerraTokenAmount, Decimal]:
        ...


async def run_strategy(
    client: TerraClient,
    arb_routes: Sequence[TerraRepeatedTxArbitrage],
    mempool_filters: Mapping[Any, Filter],
    max_n_blocks: int = None,
    log_time: bool = True,
):
    log.info(f"Running strategy with {len(arb_routes)=} and {len(mempool_filters)=}")
    start_height = client.height
    routes_by_key = {
        key: [arb_route for arb_route in arb_routes if key in arb_route.filter_keys]
        for key in mempool_filters
    }
    async for height, mempool in client.mempool.iter_height_mempool(mempool_filters):
        if log_time:
            start = time.time()
        if is_new_block := any(height > arb_route.last_run_height for arb_route in arb_routes):
            log.debug(f"New block: {height=}")
            utils.cache.clear_caches(utils.cache.CacheGroup.TERRA)
            asyncio.create_task(client.update_active_broadcaster())
        if mempool:
            n_msgs = sum(len(list_msgs) for list_msgs in mempool.values())
            log.debug(f"New mempool txs: {n_msgs=}")
        tasks: list[Awaitable] = []
        for arb_route in arb_routes:
            mempool_route = {
                k: list_msgs for k, list_msgs in mempool.items() if k in arb_route.filter_keys
            }
            if height > arb_route.last_run_height or mempool_route:
                tasks.append(arb_route.run(height, mempool_route, hold_broadcast=True))
        await asyncio.gather(*tasks)
        broadcast_tasks: dict[TerraRepeatedTxArbitrage, Awaitable] = {}
        for key, routes in routes_by_key.items():
            ready_arbs = [r for r in routes if r.state == State.ready_to_broadcast]
            if not ready_arbs:
                continue
            log.info(f"{key=} has {len(routes)} possible arbitrages")
            best_route = max(ready_arbs, key=lambda x: x.data.params.est_net_profit_usd)  # type: ignore # noqa: E501
            if best_route not in broadcast_tasks:
                broadcast_tasks[best_route] = best_route.run(height)
        await asyncio.gather(*broadcast_tasks.values())
        for route in arb_routes:
            if route.state == State.ready_to_broadcast:
                route.reset()
        if max_n_blocks is not None and (n_blocks := height - start_height) >= max_n_blocks:
            break
        if log_time:
            total_time_ms = (time.time() - start) * 1000  # type: ignore
            stats = f"{total_time_ms:.1f}ms; {len(tasks)=}, {len(broadcast_tasks)=}"
            if is_new_block:
                log.debug(f"Processed block: {height=} in {stats}")
            else:
                log.debug(f"Processed mempool txs: {n_msgs=} in {stats}")
    log.info(f"Stopped execution after {n_blocks=}")
