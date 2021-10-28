from __future__ import annotations

import asyncio
import logging
import re
import time
from decimal import Decimal
from typing import Sequence

from terra_sdk.core import Coins
from terra_sdk.core.auth import StdFee
from terra_sdk.core.broadcast import SyncTxBroadcastResult
from terra_sdk.core.msg import Msg
from terra_sdk.exceptions import LCDResponseError

import configs
from chains.terra.token import TerraNativeToken, TerraTokenAmount
from exceptions import FeeEstimationError, TxAlreadyBroadcasted
from utils.cache import CacheGroup, ttl_cache

from .base_api import Api

log = logging.getLogger(__name__)

TERRA_GAS_PRICE_CACHE_TTL = 3600
FALLBACK_EXTRA_GAS_ADJUSTMENT = Decimal("0.20")
MAX_BROADCAST_TRIES = 10
MAX_FEE_ESTIMATION_TRIES = 5

_pat_sequence_error = re.compile(r"account sequence mismatch, expected (\d+)")


class NoLogError(Exception):
    def __init__(self, data):
        self.message = data.get("raw_log", "")
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
        account_number: int = None,
        sequence: int = None,
    ) -> StdFee:
        fee_denom = self.client.fee_denom if fee_denom is None else fee_denom
        account_number, sequence = await self.client._valid_account_params(account_number, sequence)
        for i in range(1, MAX_FEE_ESTIMATION_TRIES + 1):
            try:
                fee = await self.client.lcd.tx.estimate_fee(
                    self.client.address,
                    msgs,
                    gas_adjustment=gas_adjustment,
                    fee_denoms=[fee_denom],
                    account_number=account_number,
                    sequence=sequence,
                )
            except LCDResponseError as e:
                if match := _pat_sequence_error.search(e.message):
                    if i == MAX_FEE_ESTIMATION_TRIES:
                        raise Exception(f"Fee estimation failed after {i} tries", e)
                    await self._check_msgs_in_mempool(msgs)
                    sequence = int(match.group(1))
                    log.debug(f"Retrying fee estimation with updated {sequence=}")
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
                return await self._fallback_fee_estimation(
                    estimated_gas_use, native_amount, fee_denom, gas_adjustment
                )
            else:
                self.client.account_sequence = sequence
                return fee
        raise Exception("Should never reach")

    async def _check_msgs_in_mempool(self, msgs: Sequence[Msg]):
        mempool = await self.client.mempool.fetch_mempool_msgs()
        data = [msg.to_data() for msg in msgs]
        if data in mempool:
            raise TxAlreadyBroadcasted

    async def _fallback_fee_estimation(
        self,
        estimated_gas_use: int,
        native_amount: TerraTokenAmount,
        fee_denom: str,
        gas_adjustment: Decimal = None,
    ) -> StdFee:
        assert isinstance(native_amount.token, TerraNativeToken)

        gas_adjustment = self.client.gas_adjustment if gas_adjustment is None else gas_adjustment
        gas_adjustment += FALLBACK_EXTRA_GAS_ADJUSTMENT
        adjusted_gas_use = round(estimated_gas_use * gas_adjustment)

        tax = await self.client.treasury.calculate_tax(native_amount)
        try:
            gas_price = next(
                coin for coin in self.client.lcd.gas_prices.to_list() if coin.denom == fee_denom
            )
        except StopIteration:
            raise TypeError(f"Invalid {fee_denom=}")
        gas_fee = int(gas_price.amount * adjusted_gas_use)
        amount = Coins({fee_denom: tax.int_amount + gas_fee})

        fee = StdFee(gas=adjusted_gas_use, amount=amount)
        log.debug(f"Fallback gas fee estimation: {fee}")
        return fee

    async def execute_multi_msgs(
        self,
        msgs: Sequence[Msg],
        n_repeat: int,
        expect_logs_: bool = True,
        account_number: int = None,
        sequence: int = None,
        fee: StdFee = None,
        fee_denom: str = None,
        **kwargs,
    ) -> list[tuple[float, SyncTxBroadcastResult]]:
        if self.client.use_broadcaster:
            log.info("Posting to broadcaster")
            return await self.client.broadcaster.post(msgs, n_repeat, expect_logs_, fee, fee_denom)
        account_number, sequence = await self.client._valid_account_params(account_number, sequence)
        if fee is None:
            fee = await self.estimate_fee(msgs, account_number=account_number, sequence=sequence)
        log.debug(f"Executing messages {n_repeat} time(s): {msgs}")
        results: list[tuple[float, SyncTxBroadcastResult]] = []
        for i in range(1, n_repeat + 1):
            log.debug(f"Executing message {i} if {n_repeat}")
            res = await self.execute_msgs(
                msgs, expect_logs_, account_number, sequence, fee, fee_denom, log_=False, **kwargs
            )
            results.append((time.time(), res))
            sequence = max(self.client.account_sequence, sequence + 1)
        return results

    async def execute_msgs(
        self,
        msgs: Sequence[Msg],
        expect_logs_: bool = True,
        account_number: int = None,
        sequence: int = None,
        fee: StdFee = None,
        fee_denom: str = None,
        log_: bool = True,
        **kwargs,
    ) -> SyncTxBroadcastResult:
        if self.client.use_broadcaster:
            log.info("Posting to broadcaster")
            ((timestamp, result),) = await self.client.broadcaster.post(
                msgs, n_repeat=1, expect_logs=expect_logs_, fee=fee, fee_denom=fee_denom
            )
            log.info(f"Broadcaster sent payload to blockchain at {timestamp=}")
            return result
        if log_:
            log.debug(f"Sending tx: {msgs}")
        fee_denom = self.client.fee_denom if fee_denom is None else fee_denom

        account_number, sequence = await self.client._valid_account_params(account_number, sequence)
        # Fixes bug in terraswap_sdk==1.0.0b2
        if fee is None:
            fee = await self.estimate_fee(msgs, account_number=account_number, sequence=sequence)
        for i in range(1, MAX_BROADCAST_TRIES + 1):
            signed_tx = await self.client.wallet.create_and_sign_tx(
                msgs,
                fee=fee,
                fee_denoms=[fee_denom],
                account_number=account_number,
                sequence=sequence,
                **kwargs,
            )
            payload = {
                "tx": signed_tx.to_data()["value"],
                "mode": "sync",
                "sequences": [str(sequence)],
            }
            try:
                res = await self.client.lcd_http_client.post("txs", json=payload)
                data: dict = res.json()
                if expect_logs_ and data.get("logs") is None:
                    raise NoLogError(data)
            except (NoLogError, LCDResponseError) as e:
                if i == MAX_BROADCAST_TRIES:
                    raise Exception(f"Broadcast failed after {i} tries", e)
                if match := _pat_sequence_error.search(e.message):
                    await self._check_msgs_in_mempool(msgs)
                    sequence = int(match.group(1))
                    log.debug(f"Retrying broadcast with updated {sequence=}")
                else:
                    raise e
            else:
                self.client.account_sequence = sequence + 1
                await self._broadcast_async(payload)
                break

        log.debug(f"Tx executed: {data['txhash']}")
        return SyncTxBroadcastResult(
            txhash=data["txhash"],
            raw_log=data.get("raw_log"),
            code=data.get("code"),
            codespace=data.get("codespace"),
        )

    async def _broadcast_async(self, payload: dict):
        payload["mode"] = "async"
        tasks = (client.post("txs", json=payload) for client in self.client.broadcast_lcd_clients)
        await asyncio.gather(*tasks)
