from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Sequence

from cosmos_proto.terra.tx.v1beta1 import ServiceStub
from cosmos_sdk.core import Coins
from cosmos_sdk.core.coin import Coin
from cosmos_sdk.core.fee import Fee
from cosmos_sdk.core.msg import Msg
from cosmos_sdk.core.tx import Tx

import configs
from exceptions import FeeEstimationError
from utils.cache import CacheGroup, ttl_cache

from ...client.api_tx import TxApi as CosmosTxApi
from ..denoms import LUNA
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
    def start(self):
        super().start()
        self.grpc_service_terra = ServiceStub(self.client.grpc_channel)

    @ttl_cache(CacheGroup.TERRA, ttl=_TERRA_GAS_PRICE_CACHE_TTL)
    async def get_gas_prices(self) -> Coins:
        res = await self.client.fcd_client.get("v1/txs/gas_prices")
        adjusted_prices = {
            denom: str(Decimal(amount) * configs.TERRA_GAS_MULTIPLIER)
            for denom, amount in res.json().items()
        }
        return Coins(adjusted_prices)

    async def _fee_estimation(self, tx: Tx, gas_prices: Coins, gas_adjustment: Decimal) -> Fee:
        tx_proto = tx.to_proto()
        res_simulation, res_tax = await asyncio.gather(
            self.grpc_service.simulate(tx=tx_proto),
            self.grpc_service_terra.compute_tax(tx_bytes=bytes(tx_proto)),
        )

        gas = int(res_simulation.gas_info.gas_used * gas_adjustment)
        fee_amount = (gas_prices * gas).to_int_coins()
        tax_amount = Coins.from_proto(res_tax.tax_amount)

        return Fee(gas, fee_amount + tax_amount)

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
                coins_send: list[Coin] = [
                    c for c in msgs[0].coins.to_list() if c.denom != LUNA.denom
                ]
            except AttributeError:
                raise FeeEstimationError("Could not get native_amount from msg")
            if not coins_send:
                native_amount = LUNA.to_amount(0)
            elif not len(coins_send) == 1:
                native_amount = TerraTokenAmount.from_coin(coins_send[0])
            else:
                raise NotImplementedError

        assert isinstance(native_amount.token, TerraNativeToken)

        gas_adjustment = (
            self.client.gas_adjustment if gas_adjustment is None else gas_adjustment
        )
        gas_adjustment += _FALLBACK_EXTRA_GAS_ADJUSTMENT
        gas_limit = round(estimated_gas_use * gas_adjustment)

        tax = await self.client.treasury.calculate_tax(native_amount)
        try:
            gas_price = next(
                coin for coin in self.client.gas_prices.to_list() if coin.denom == fee_denom
            )
        except StopIteration:
            raise TypeError(f"Invalid {fee_denom=}")
        gas_fee = int(gas_price.amount * gas_limit)
        amount = Coins({fee_denom: tax.int_amount + gas_fee})

        fee = Fee(gas_limit, amount)
        log.debug(f"Fallback gas fee estimation: {fee}")
        return fee
