from __future__ import annotations

import asyncio
import base64
import logging
import re
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from copy import copy
from decimal import Decimal
from typing import Generic, Sequence, cast

import grpclib
from cosmos_proto.cosmos.tx.v1beta1 import BroadcastMode, ServiceStub
from cosmos_sdk.client.lcd.api.tx import CreateTxOptions, SignerOptions
from cosmos_sdk.core import Coins
from cosmos_sdk.core.broadcast import SyncTxBroadcastResult
from cosmos_sdk.core.fee import Fee
from cosmos_sdk.core.msg import Msg
from cosmos_sdk.core.tx import AuthInfo, SignerData, Tx, TxBody
from cosmos_sdk.exceptions import LCDResponseError

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
    def start(self):
        self.grpc_service = ServiceStub(self.client.grpc_channel)

    async def estimate_fee(
        self,
        msgs: Sequence[Msg],
        gas_adjustment: Decimal = None,
        use_fallback_estimate: bool = True,
        estimated_gas_use: int = None,
        fee_denom: str = None,
        **kwargs,
    ) -> Fee:
        fee_denom = fee_denom or self.client.fee_denom
        gas_prices = Coins([self.client.gas_prices[fee_denom]])
        gas_adjustment = gas_adjustment or self.client.gas_adjustment
        signer = self.client.signer
        for i in range(1, _MAX_FEE_ESTIMATION_TRIES + 1):
            tx = self.get_unsigned_tx(msgs, [fee_denom], [signer])
            try:
                fee = await self._fee_estimation(tx, gas_prices, gas_adjustment)
            except grpclib.GRPCError as e:
                error_msg = e.message or ""
                if match := _pat_sequence_error.search(error_msg):
                    if i == _MAX_FEE_ESTIMATION_TRIES:
                        raise Exception(f"Fee estimation failed after {i} tries", e)
                    await self._check_msgs_in_mempool(msgs)
                    signer = copy(signer)
                    signer.sequence = int(match.group(1))
                    log.debug(f"Retrying fee estimation with updated {signer.sequence=}")
                    continue
                if not use_fallback_estimate:
                    raise FeeEstimationError(e)
                if estimated_gas_use is None:
                    raise FeeEstimationError(
                        "Could not use fallback fee estimation without estimated_gas_use", e
                    )
                try:
                    return await self._fallback_fee_estimation(
                        estimated_gas_use,
                        gas_adjustment,
                        fee_denom,
                        msgs,
                        **kwargs,
                    )
                except Exception as e:
                    raise FeeEstimationError("Error on fallback fee estimation", e)
            else:
                self.client.signer = signer
                return fee
        raise Exception("Should never reach")

    async def _fee_estimation(self, tx: Tx, gas_prices: Coins, gas_adjustment: Decimal) -> Fee:
        sim = await self.grpc_service.simulate(tx=tx.to_proto())
        gas = int(sim.gas_info.gas_used * gas_adjustment)
        fee_amount = (gas_prices * gas).to_int_coins()

        return Fee(gas, fee_amount)

    def get_unsigned_tx(
        self,
        msgs: Sequence[Msg],
        fee_denoms: list[str] = None,
        signer_opts: list[SignerOptions] = None,
    ) -> Tx:
        fee_denoms = [self.client.fee_denom] if fee_denoms is None else fee_denoms
        signer_opts = [self.client.signer] if signer_opts is None else signer_opts

        tx_body = TxBody(messages=list(msgs), memo="")
        coins_fee = Coins(",".join(f"0{denom}" for denom in fee_denoms))
        auth_info = AuthInfo([], Fee(0, coins_fee))

        tx = Tx(tx_body, auth_info, [])
        signers = cast(list[SignerData], signer_opts)
        tx.append_empty_signatures(signers)
        return tx

    async def get_simulation_events(self, msgs: Sequence[Msg]) -> dict[str, list[dict]]:
        tx = self.client.tx.get_unsigned_tx(msgs)
        res = await self.grpc_service.simulate(tx=tx.to_proto())
        events: dict[str, list[dict]] = defaultdict(list)
        for e in res.result.events:
            events[e.type].append(
                {a.key.decode("utf-8"): a.value.decode("utf-8") for a in e.attributes}
            )
        return dict(events)

    @abstractmethod
    async def _fallback_fee_estimation(
        self,
        estimated_gas_use: int,
        gas_adjustment: Decimal,
        fee_denom: str,
        msgs: Sequence[Msg],
        **kwargs,
    ) -> Fee:
        ...

    async def _check_msgs_in_mempool(self, msgs: Sequence[Msg]):
        mempool = await self.client.mempool.fetch_mempool_txs()
        data = [msg.to_data() for msg in msgs]
        if data in mempool:
            raise TxAlreadyBroadcasted("Tx in mempool")

    async def execute_multi_msgs(
        self,
        msgs: Sequence[Msg],
        n_repeat: int,
        fee: Fee = None,
        fee_denom: str = None,
    ) -> list[tuple[float, SyncTxBroadcastResult]]:
        if self.client.active_broadcaster is not None:
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
        msgs: Sequence[Msg],
        fee: Fee = None,
        fee_denom: str = None,
        log_: bool = True,
    ) -> SyncTxBroadcastResult:
        if self.client.active_broadcaster is not None:
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
                res = await self.grpc_service.broadcast_tx(
                    tx_bytes=bytes(tx.to_proto()), mode=BroadcastMode.BROADCAST_MODE_SYNC
                )
                if res.tx_response.code:
                    raise BroadcastError(res.tx_response)
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
                last_sequence = max(signer.sequence or 0, self.client.signer.sequence or 0)
                self.client.signer.sequence = last_sequence + 1
                asyncio.create_task(self._broadcast_async(tx))
                log.debug(f"Tx executed: {res.tx_response.txhash}")
                return SyncTxBroadcastResult(
                    txhash=res.tx_response.txhash,
                    raw_log=res.tx_response.raw_log,
                    code=res.tx_response.code,
                    codespace=res.tx_response.codespace,
                )
        raise Exception("Should never reach")

    async def _broadcast_async(self, tx: Tx):
        data = {
            "tx_bytes": base64.b64encode(bytes(tx.to_proto())).decode("ascii"),
            "mode": "BROADCAST_MODE_ASYNC",
        }
        tasks = (
            client.post("cosmos/tx/v1beta1/txs", json=data, n_tries=2)
            for client in self.client.broadcast_lcd_clients
        )
        res = await asyncio.gather(*tasks, return_exceptions=True)
        for e in res:
            if isinstance(e, Exception):
                log.debug(f"Error on async broadcast: {e!r}")
