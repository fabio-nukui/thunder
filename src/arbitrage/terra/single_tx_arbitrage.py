import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from terra_sdk.core.auth import StdFee, TxInfo
from terra_sdk.core.wasm import MsgExecuteContract
from terra_sdk.exceptions import LCDResponseError

import utils
from chains.terra import TerraClient, TerraTokenAmount
from chains.terra.tx_filter import Filter
from exceptions import BlockchainNewState, IsBusy

from ..single_tx_arbitrage import ArbResult, ArbTx, BaseArbParams, SingleTxArbitrage, TxStatus

log = logging.getLogger(__name__)

MIN_CONFIRMATIONS = 1
MAX_BLOCKS_WAIT_RECEIPT = 10


class TerraArbParams(BaseArbParams):
    timestamp_found: float
    block_found: int

    msgs: list[MsgExecuteContract]
    est_fee: StdFee


class TerraSingleTxArbitrage(SingleTxArbitrage[TerraClient], ABC):
    def __init__(self, *args, filter_keys: Iterable, **kwargs):
        self.filter_keys = filter_keys
        super().__init__(*args, **kwargs)

    async def _broadcast_tx(self, arb_params: TerraArbParams, height: int) -> ArbTx:
        if (latest_height := await self.client.get_latest_height()) != height:
            raise BlockchainNewState(f"{latest_height=} different from {height=}")
        res = await self.client.tx.execute_msgs(arb_params.msgs, fee=arb_params.est_fee)
        return ArbTx(timestamp_sent=time.time(), tx_hash=res.txhash)

    async def _confirm_tx(self, height: int) -> ArbResult:
        assert self.data.params is not None
        assert self.data.tx is not None
        tx_inclusion_delay = height - self.data.params.block_found
        try:
            info = await self.client.lcd.tx.tx_info(self.data.tx.tx_hash)
        except LCDResponseError as e:
            if e.response.status == 404:
                if tx_inclusion_delay >= MAX_BLOCKS_WAIT_RECEIPT:
                    return ArbResult(TxStatus.not_found)
                raise IsBusy
            raise
        if height - info.height < MIN_CONFIRMATIONS:
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
    arb_routes: Sequence[TerraSingleTxArbitrage],
    mempool_filters: Mapping[Any, Filter],
    max_n_blocks: int = None,
):
    start_height = client.height
    async for height, mempool in client.mempool.iter_height_mempool(mempool_filters):
        if any(height > arb_route.last_run_height for arb_route in arb_routes):
            utils.cache.clear_caches(utils.cache.CacheGroup.TERRA)
        for arb_route in arb_routes:
            mempool_route = {
                key: filter_ for key, filter_ in mempool.items() if key in arb_route.filter_keys
            }
            any_new_mempool_msg = any(list_msgs for list_msgs in mempool_route.values())
            if height > arb_route.last_run_height or any_new_mempool_msg:
                await arb_route.run(height, mempool_route)
        if max_n_blocks is not None and (n_blocks := height - start_height) >= max_n_blocks:
            break
    log.info(f"Stopped execution after {n_blocks=}")
