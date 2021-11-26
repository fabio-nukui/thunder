from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from terra_sdk.core import Coins
from terra_sdk.core.fee import Fee

from ...client.api_tx import TxApi as CosmosTxApi

if TYPE_CHECKING:
    from .async_client import OsmosisClient  # noqa: F401

log = logging.getLogger(__name__)

_FALLBACK_EXTRA_GAS_ADJUSTMENT = Decimal("0.20")


class TxApi(CosmosTxApi["OsmosisClient"]):
    async def _fallback_fee_estimation(
        self,
        estimated_gas_use: int,
        gas_adjustment: Decimal,
        *args,
        **kwargs,
    ) -> Fee:
        gas_adjustment += _FALLBACK_EXTRA_GAS_ADJUSTMENT
        return Fee(int(estimated_gas_use * gas_adjustment), Coins("uosmo0"))
