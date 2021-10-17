from __future__ import annotations

import logging
import re
from decimal import Decimal

from terra_sdk.core import Coins
from terra_sdk.core.auth import StdFee
from terra_sdk.core.broadcast import AsyncTxBroadcastResult
from terra_sdk.core.coin import Coin
from terra_sdk.core.msg import Msg
from terra_sdk.exceptions import LCDResponseError

import configs
from chains.terra.token import TerraNativeToken, TerraTokenAmount
from exceptions import EstimateFeeError
from utils.cache import CacheGroup, ttl_cache

from ..interfaces import ITxApi

log = logging.getLogger(__name__)

TERRA_GAS_PRICE_CACHE_TTL = 3600
FALLBACK_EXTRA_GAS_ADJUSTMENT = Decimal("0.1")

_pat_sequence_error = re.compile(r"account sequence mismatch, expected (\d+)")


class TxApi(ITxApi):
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=TERRA_GAS_PRICE_CACHE_TTL)
    async def get_gas_prices(self) -> Coins:
        res = await self.client.fcd_client.get("v1/txs/gas_prices")
        coin_list = [
            Coin(denom=denom, amount=str(Decimal(amount) * configs.TERRA_GAS_MULTIPLIER_PREMIUM))
            for denom, amount in res.json().items()
        ]
        return Coins(coin_list)

    async def estimate_fee(
        self,
        msgs: list[Msg],
        gas_adjustment: Decimal = None,
        use_fallback_estimate: bool = False,
        estimated_gas_use: int = None,
        native_amount: TerraTokenAmount = None,
    ) -> StdFee:
        try:
            return await self.client.lcd.tx.estimate_fee(
                self.client.address,
                msgs,
                gas_adjustment=gas_adjustment,
                fee_denoms=[self.client.fee_denom],
            )
        except LCDResponseError as e:
            if not (use_fallback_estimate or "account sequence mismatch" in e.message):
                raise e
            if estimated_gas_use is None:
                raise EstimateFeeError(
                    "Could not use fallback fee estimaion without estimated_gas_use", e
                )
            if native_amount is None:
                coins_send: Coins | None = getattr(msgs[0], "coins", None)
                if coins_send:
                    if not len(coins_send) == 1:
                        raise NotImplementedError
                    native_amount = TerraTokenAmount.from_coin(coins_send.to_list()[0])
                else:
                    raise EstimateFeeError("Could not get native_amount from msg", e)
        return await self.fallback_fee_estimation(estimated_gas_use, native_amount, gas_adjustment)

    async def fallback_fee_estimation(
        self,
        estimated_gas_use: int,
        native_amount: TerraTokenAmount,
        gas_adjustment: Decimal = None,
    ) -> StdFee:
        assert isinstance(native_amount.token, TerraNativeToken)
        assert native_amount.token.denom == self.client.fee_denom

        gas_adjustment = self.client.gas_adjustment if gas_adjustment is None else gas_adjustment
        gas_adjustment += FALLBACK_EXTRA_GAS_ADJUSTMENT
        adjusted_gas_use = estimated_gas_use * gas_adjustment

        tax = await self.client.treasury.calculate_tax(native_amount)
        gas_price = next(
            coin
            for coin in self.client.lcd.gas_prices.to_list()
            if coin.denom == self.client.fee_denom
        )
        gas_fee = int(gas_price.amount * adjusted_gas_use)
        amount = Coins([Coin(denom=self.client.fee_denom, amount=tax.int_amount + gas_fee)])

        fee = StdFee(gas=adjusted_gas_use, amount=amount)
        log.debug(f"Fallback gas fee estimation: {fee}")
        return fee

    async def execute_msgs(self, msgs: list[Msg], **kwargs) -> AsyncTxBroadcastResult:
        log.debug(f"Sending tx: {msgs}")

        # Fixes bug in terraswap_sdk==1.0.0b2
        if "fee" not in kwargs:
            kwargs["fee"] = self.estimate_fee(msgs)

        while True:
            signed_tx = await self.client.wallet.create_and_sign_tx(
                msgs,
                fee_denoms=[self.client.fee_denom],
                **kwargs,
            )
            payload = {"tx": signed_tx.to_data()["value"], "mode": "async"}
            try:
                res = (await self.client.lcd_http_client.post("txs", json=payload)).json()
                break
            except LCDResponseError as e:
                if match := _pat_sequence_error.search(e.message):
                    kwargs["sequence"] = int(match.group(1))
        log.debug(f"Tx executed: {res['txhash']}")
        return AsyncTxBroadcastResult(txhash=res["txhash"], height=self.client.height)
