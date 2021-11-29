from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, cast

from cosmos_proto.cosmos.tx.v1beta1 import ServiceStub
from cosmos_sdk.client.lcd.api.tx import CreateTxOptions, SignerOptions
from cosmos_sdk.core import Coins
from cosmos_sdk.core.fee import Fee
from cosmos_sdk.core.tx import AuthInfo, SignerData, Tx, TxBody

from ...client.api_tx import TxApi as CosmosTxApi
from ..denoms import OSMO

if TYPE_CHECKING:
    from .async_client import OsmosisClient  # noqa: F401

log = logging.getLogger(__name__)

_FALLBACK_EXTRA_GAS_ADJUSTMENT = Decimal("0.20")


class TxApi(CosmosTxApi["OsmosisClient"]):
    def start(self):
        self.grpc_service = ServiceStub(self.client.grpc_channel)

    async def _fee_estimation(
        self,
        signer_opts: list[SignerOptions],
        options: CreateTxOptions,
    ) -> Fee:
        gas_prices = options.gas_prices or self.client.gas_prices
        gas_adjustment = options.gas_adjustment or self.client.gas_adjustment

        tx_body = TxBody(messages=options.msgs, memo=options.memo or "")
        auth_info = AuthInfo([], Fee(0, Coins()))

        tx = Tx(tx_body, auth_info, [])
        signers = cast(list[SignerData], signer_opts)
        tx.append_empty_signatures(signers)

        sim = await self.grpc_service.simulate(tx=tx.to_proto())
        gas = int(sim.gas_info.gas_used * gas_adjustment)
        fee_amount = gas_prices * gas / 10 ** 6

        return Fee(gas, fee_amount)

    async def _fallback_fee_estimation(
        self,
        estimated_gas_use: int,
        gas_adjustment: Decimal,
        *args,
        **kwargs,
    ) -> Fee:
        gas_adjustment += _FALLBACK_EXTRA_GAS_ADJUSTMENT
        return Fee(int(estimated_gas_use * gas_adjustment), Coins(f"0{OSMO.denom}"))
