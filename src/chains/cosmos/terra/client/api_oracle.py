from __future__ import annotations

from decimal import Decimal

from terra_proto.terra.oracle.v1beta1 import QueryStub

from utils.cache import CacheGroup, ttl_cache

from ..denoms import LUNA
from ..token import TerraNativeToken
from .base_api import Api

PRECISION = 18


class OracleApi(Api):
    def start(self):
        self.grpc_query = QueryStub(self.client.grpc_channel)

    @ttl_cache(CacheGroup.TERRA, maxsize=1)
    async def get_exchange_rates(self) -> dict[TerraNativeToken, Decimal]:
        res = await self.grpc_query.exchange_rates()
        rates = {
            TerraNativeToken(c.denom): Decimal(c.amount) / 10 ** PRECISION
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
        return round(exchange_rates[to_coin] / exchange_rates[from_coin], PRECISION)
