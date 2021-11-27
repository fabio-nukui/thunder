from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Sequence

from terra_sdk.client.lcd.api.tx import CreateTxOptions, SignerOptions
from terra_sdk.core import Coins
from terra_sdk.core.fee import Fee

import configs
from chains.cosmos.msg import Msg
from exceptions import FeeEstimationError
from utils.cache import CacheGroup, ttl_cache

from ...client.api_tx import TxApi as CosmosTxApi
from ..token import TerraNativeToken, TerraTokenAmount

if TYPE_CHECKING:
    from .async_client import TerraClient  # noqa: F401

log = logging.getLogger(__name__)

_TERRA_GAS_PRICE_CACHE_TTL = 3600
_FALLBACK_EXTRA_GAS_ADJUSTMENT = Decimal("0.20")


class BroadcastError(Exception):
    def __init__(self, data):
        self.message = getattr(data, "raw_log", "")
        super().__init__(data)


class TxApi(CosmosTxApi["TerraClient"]):
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=_TERRA_GAS_PRICE_CACHE_TTL)
    async def get_gas_prices(self) -> Coins:
        res = await self.client.fcd_client.get("v1/txs/gas_prices")
        adjusted_prices = {
            denom: str(Decimal(amount) * configs.TERRA_GAS_MULTIPLIER)
            for denom, amount in res.json().items()
        }
        return Coins(adjusted_prices)

    async def _fee_estimation(
        self,
        signers: list[SignerOptions],
        options: CreateTxOptions,
    ) -> Fee:
        return await self.client.lcd.tx.estimate_fee(signers, options)

    async def _fallback_fee_estimation(
        self,
        estimated_gas_use: int,
        gas_adjustment: Decimal,
        fee_denom: str,
        msgs: Sequence[Msg],
        native_amount: TerraTokenAmount = None,
        **kwargs,
    ) -> Fee:
        if native_amount is None:
            try:
                coins_send: Coins = msgs[0].coins
            except AttributeError:
                raise FeeEstimationError("Could not get native_amount from msg")
            if not len(coins_send) == 1:
                raise NotImplementedError
            native_amount = TerraTokenAmount.from_coin(coins_send.to_list()[0])

        assert isinstance(native_amount.token, TerraNativeToken)

        gas_adjustment = (
            self.client.gas_adjustment if gas_adjustment is None else gas_adjustment
        )
        gas_adjustment += _FALLBACK_EXTRA_GAS_ADJUSTMENT
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
