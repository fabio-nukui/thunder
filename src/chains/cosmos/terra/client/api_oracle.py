from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from cosmos_proto.terra.oracle.v1beta1 import QueryStub

from utils.cache import CacheGroup, ttl_cache

from ...client.base_api import Api
from ..denoms import LUNA
from ..token import TerraNativeToken
from .utils import parse_proto_decimal

if TYPE_CHECKING:
    from .async_client import TerraClient  # noqa: F401

_PRECISION = 18
_PARAMETERS_TTL = 3600


class OracleApi(Api["TerraClient"]):
    def start(self):
        self.grpc_query = QueryStub(self.client.grpc_channel)

    @ttl_cache(CacheGroup.TERRA)
    async def get_exchange_rates(self) -> dict[TerraNativeToken, Decimal]:
        res = await self.grpc_query.exchange_rates()
        rates = {
            TerraNativeToken(c.denom): Decimal(c.amount) / 10 ** _PRECISION
            for c in res.exchange_rates
        }
        rates[LUNA] = Decimal(1)
        return rates

    async def get_exchange_rate(
        self,
        from_coin: TerraNativeToken | str,
        to_coin: TerraNativeToken | str,
    ) -> Decimal:
        if isinstance(from_coin, str):
            from_coin = TerraNativeToken(from_coin)
        if isinstance(to_coin, str):
            to_coin = TerraNativeToken(to_coin)
        exchange_rates = await self.get_exchange_rates()
        return round(exchange_rates[to_coin] / exchange_rates[from_coin], _PRECISION)

    @ttl_cache(CacheGroup.TERRA, ttl=_PARAMETERS_TTL)
    async def get_tobin_taxes(self) -> dict[TerraNativeToken, Decimal]:
        res = await self.grpc_query.params()
        return {
            TerraNativeToken(item.name): parse_proto_decimal(item.tobin_tax)
            for item in res.params.whitelist
        }
