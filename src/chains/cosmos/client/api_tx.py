from __future__ import annotations

import asyncio
import logging
import re
import time
from abc import ABC, abstractmethod
from copy import copy
from decimal import Decimal
from typing import Generic, Sequence

from terra_sdk.client.lcd.api.tx import CreateTxOptions, SignerOptions
from terra_sdk.core.broadcast import SyncTxBroadcastResult
from terra_sdk.core.fee import Fee
from terra_sdk.core.tx import Tx
from terra_sdk.exceptions import LCDResponseError

from chains.cosmos.msg import MsgType
from exceptions import FeeEstimationError, TxAlreadyBroadcasted

from .base_api import Api, CosmosClientT

log = logging.getLogger(__name__)

_MAX_BROADCAST_TRIES = 10
_MAX_FEE_ESTIMATION_TRIES = 5

_pat_sequence_error = re.compile(r"account sequence mismatch, expected (\d+)")


class BroadcastError(Exception):
    def __init__(self, data):
        self.message = getattr(data, "raw_log", "")
        super().__init__(data)


class TxApi(Generic[CosmosClientT], Api[CosmosClientT], ABC):
    async def estimate_fee(
        self,
        msgs: Sequence[MsgType],
        gas_adjustment: Decimal = None,
        use_fallback_estimate: bool = False,
        estimated_gas_use: int = None,
        fee_denom: str = None,
        **kwargs,
    ) -> Fee:
        fee_denom = fee_denom or self.client.fee_denom
        gas_adjustment = gas_adjustment or self.client.gas_adjustment
        signer = self.client.signer
        for i in range(1, _MAX_FEE_ESTIMATION_TRIES + 1):
            create_tx_options = CreateTxOptions(
                msgs,
                gas_prices=self.client.gas_prices,
                gas_adjustment=gas_adjustment,
                fee_denoms=[fee_denom],
                sequence=signer.sequence,
            )
            try:
                fee = await self._fee_estimation([signer], create_tx_options)
            except LCDResponseError as e:
                if match := _pat_sequence_error.search(e.message):
                    if i == _MAX_FEE_ESTIMATION_TRIES:
                        raise Exception(f"Fee estimation failed after {i} tries", e)
                    await self._check_msgs_in_mempool(msgs)
                    signer = copy(signer)
                    signer.sequence = int(match.group(1))
                    log.debug(f"Retrying fee estimation with updated {signer.sequence=}")
                    continue
                if not use_fallback_estimate:
                    raise e
                if "spread assertion" in e.message:
                    raise FeeEstimationError(e.message)
                if estimated_gas_use is None:
                    raise FeeEstimationError(
                        "Could not use fallback fee estimation without estimated_gas_use", e
                    )
                return await self._fallback_fee_estimation(
                    estimated_gas_use,
                    gas_adjustment,
                    fee_denom,
                    msgs,
                    **kwargs,
                )
            else:
                self.client.signer = signer
                return fee
        raise Exception("Should never reach")

    @abstractmethod
    async def _fee_estimation(
        self,
        signers: list[SignerOptions],
        options: CreateTxOptions,
    ) -> Fee:
        ...

    @abstractmethod
    async def _fallback_fee_estimation(
        self,
        estimated_gas_use: int,
        gas_adjustment: Decimal,
        fee_denom: str,
        msgs: Sequence[MsgType],
        **kwargs,
    ) -> Fee:
        ...

    async def _check_msgs_in_mempool(self, msgs: Sequence[MsgType]):
        mempool = await self.client.mempool.fetch_mempool_txs()
        data = [msg.to_data() for msg in msgs]
        if data in mempool:
            raise TxAlreadyBroadcasted("Tx in mempool")

    async def execute_multi_msgs(
        self,
        msgs: Sequence[MsgType],
        n_repeat: int,
        fee: Fee = None,
        fee_denom: str = None,
    ) -> list[tuple[float, SyncTxBroadcastResult]]:
        if self.client.use_broadcaster:
            return await self.client.broadcaster.post(msgs, n_repeat, fee, fee_denom)
        log.info("Broadcasting with local LCD")
        if fee is None:
            fee = await self.estimate_fee(msgs, fee_denom=fee_denom)
        log.debug(f"Executing messages {n_repeat} time(s): {msgs}")
        results: list[tuple[float, SyncTxBroadcastResult]] = []
        for i in range(1, n_repeat + 1):
            log.debug(f"Executing message {i} if {n_repeat}")
            res = await self.execute_msgs(msgs, fee, fee_denom, log_=False)
            results.append((time.time(), res))
        return results

    async def execute_msgs(
        self,
        msgs: Sequence[MsgType],
        fee: Fee = None,
        fee_denom: str = None,
        log_: bool = True,
    ) -> SyncTxBroadcastResult:
        if self.client.use_broadcaster:
            ((timestamp, result),) = await self.client.broadcaster.post(
                msgs, n_repeat=1, fee=fee, fee_denom=fee_denom
            )
            log.info(f"Broadcaster sent payload to blockchain at {timestamp=}")
            return result
        if log_:
            log.debug(f"Sending tx: {msgs}")
        fee_denom = self.client.fee_denom if fee_denom is None else fee_denom
        if fee is None:
            fee = await self.estimate_fee(msgs, self.client.gas_adjustment, fee_denom=fee_denom)

        signer = self.client.signer
        for i in range(1, _MAX_BROADCAST_TRIES + 1):
            create_tx_options = CreateTxOptions(
                msgs, fee, fee_denoms=[fee_denom], sequence=signer.sequence
            )
            tx = await self.client.wallet.create_and_sign_tx([signer], create_tx_options)
            try:
                res = await self.client.lcd.tx.broadcast_sync(tx)
                if res.is_tx_error():
                    raise BroadcastError(res)
            except (BroadcastError, LCDResponseError) as e:
                if i == _MAX_BROADCAST_TRIES:
                    raise Exception(f"Broadcast failed after {i} tries", e)
                if match := _pat_sequence_error.search(e.message):
                    await self._check_msgs_in_mempool(msgs)
                    signer = copy(signer)
                    signer.sequence = int(match.group(1))
                    log.debug(f"Retrying broadcast with updated {signer.sequence=}")
                else:
                    raise e
            else:
                self.client.signer.sequence = (signer.sequence or 0) + 1
                asyncio.create_task(self._broadcast_async(tx))
                log.debug(f"Tx executed: {res.txhash}")
                return res
        raise Exception("Should never reach")

    async def _broadcast_async(self, tx: Tx):
        data = {"tx_bytes": self.client.lcd.tx.encode(tx), "mode": "BROADCAST_MODE_ASYNC"}
        tasks = (
            client.post("cosmos/tx/v1beta1/txs", json=data, n_tries=2)
            for client in self.client.broadcast_lcd_clients
        )
        res = await asyncio.gather(*tasks, return_exceptions=True)
        for e in res:
            if isinstance(e, Exception):
                log.debug(f"Error on async broadcast: {e!r}")
