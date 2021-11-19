from __future__ import annotations

import asyncio
import logging
import re
import time
from copy import copy
from decimal import Decimal
from typing import Sequence

from terra_sdk.client.lcd.api.tx import CreateTxOptions
from terra_sdk.core import Coins
from terra_sdk.core.broadcast import SyncTxBroadcastResult
from terra_sdk.core.fee import Fee
from terra_sdk.core.msg import Msg
from terra_sdk.core.tx import Tx
from terra_sdk.exceptions import LCDResponseError

import configs
from exceptions import FeeEstimationError, TxAlreadyBroadcasted
from utils.cache import CacheGroup, ttl_cache

from ..token import TerraNativeToken, TerraTokenAmount
from .base_api import Api

log = logging.getLogger(__name__)

TERRA_GAS_PRICE_CACHE_TTL = 3600
FALLBACK_EXTRA_GAS_ADJUSTMENT = Decimal("0.20")
MAX_BROADCAST_TRIES = 10
MAX_FEE_ESTIMATION_TRIES = 5

_pat_sequence_error = re.compile(r"account sequence mismatch, expected (\d+)")


class BroadcastError(Exception):
    def __init__(self, data):
        self.message = getattr(data, "raw_log", "")
        super().__init__(data)


class TxApi(Api):
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_GAS_PRICE_CACHE_TTL)
    async def get_gas_prices(self) -> Coins:
        res = await self.client.fcd_client.get("v1/txs/gas_prices")
        adjusted_prices = {
            denom: str(Decimal(amount) * configs.TERRA_GAS_MULTIPLIER)
            for denom, amount in res.json().items()
        }
        return Coins(adjusted_prices)

    async def estimate_fee(
        self,
        msgs: Sequence[Msg],
        gas_adjustment: Decimal = None,
        use_fallback_estimate: bool = False,
        estimated_gas_use: int = None,
        native_amount: TerraTokenAmount = None,
        fee_denom: str = None,
    ) -> Fee:
        fee_denom = fee_denom or self.client.fee_denom
        gas_adjustment = gas_adjustment or self.client.gas_adjustment
        signer = self.client.signer
        for i in range(1, MAX_FEE_ESTIMATION_TRIES + 1):
            create_tx_options = CreateTxOptions(
                msgs,
                gas_prices=self.client.gas_prices,
                gas_adjustment=gas_adjustment,
                fee_denoms=[fee_denom],
                sequence=signer.sequence,
            )
            try:
                fee = await self.client.lcd.tx.estimate_fee([signer], create_tx_options)
            except LCDResponseError as e:
                if match := _pat_sequence_error.search(e.message):
                    if i == MAX_FEE_ESTIMATION_TRIES:
                        raise Exception(f"Fee estimation failed after {i} tries", e)
                    await self._check_msgs_in_mempool(msgs)
                    signer = copy(signer)
                    signer.sequence = int(match.group(1))
                    log.debug(f"Retrying fee estimation with updated {signer.sequence=}")
                    continue
                if not use_fallback_estimate:
                    raise e
                if estimated_gas_use is None:
                    raise FeeEstimationError(
                        "Could not use fallback fee estimation without estimated_gas_use", e
                    )
                if native_amount is None:
                    coins_send: Coins | None = getattr(msgs[0], "coins", None)
                    if coins_send:
                        if not len(coins_send) == 1:
                            raise NotImplementedError
                        native_amount = TerraTokenAmount.from_coin(coins_send.to_list()[0])
                    else:
                        raise FeeEstimationError("Could not get native_amount from msg", e)
                log.debug(f"Trying fallback fee estimation({e.message=})")
                return await self._fallback_fee_estimation(
                    estimated_gas_use, native_amount, fee_denom, gas_adjustment
                )
            else:
                self.client.signer = signer
                return fee
        raise Exception("Should never reach")

    async def _check_msgs_in_mempool(self, msgs: Sequence[Msg]):
        mempool = await self.client.mempool.fetch_mempool_msgs()
        data = [msg.to_data() for msg in msgs]
        if data in mempool:
            raise TxAlreadyBroadcasted("Tx in mempool")

    async def _fallback_fee_estimation(
        self,
        estimated_gas_use: int,
        native_amount: TerraTokenAmount,
        fee_denom: str,
        gas_adjustment: Decimal = None,
    ) -> Fee:
        assert isinstance(native_amount.token, TerraNativeToken)

        gas_adjustment = (
            self.client.gas_adjustment if gas_adjustment is None else gas_adjustment
        )
        gas_adjustment += FALLBACK_EXTRA_GAS_ADJUSTMENT
        gas_limit = round(estimated_gas_use * gas_adjustment)

        tax = await self.client.treasury.calculate_tax(native_amount)
        try:
            gas_price = next(
                coin for coin in self.client.lcd.gas_prices.to_list() if coin.denom == fee_denom
            )
        except StopIteration:
            raise TypeError(f"Invalid {fee_denom=}")
        gas_fee = int(gas_price.amount * gas_limit)
        amount = Coins({fee_denom: tax.int_amount + gas_fee})

        fee = Fee(gas_limit, amount)
        log.debug(f"Fallback gas fee estimation: {fee}")
        return fee

    async def execute_multi_msgs(
        self,
        msgs: Sequence[Msg],
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
        msgs: Sequence[Msg],
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
        for i in range(1, MAX_BROADCAST_TRIES + 1):
            create_tx_options = CreateTxOptions(
                msgs, fee, fee_denoms=[fee_denom], sequence=signer.sequence
            )
            tx = await self.client.wallet.create_and_sign_tx([signer], create_tx_options)
            try:
                res = await self.client.lcd.tx.broadcast_sync(tx)
                if res.is_tx_error():
                    raise BroadcastError(res)
            except (BroadcastError, LCDResponseError) as e:
                if i == MAX_BROADCAST_TRIES:
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
