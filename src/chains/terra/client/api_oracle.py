from __future__ import annotations

from decimal import Decimal

from utils.cache import CacheGroup, ttl_cache

from ..denoms import LUNA
from ..interfaces import IOracleApi
from ..token import TerraNativeToken

MAX_PRECISION = 18


class OracleApi(IOracleApi):
    @ttl_cache(CacheGroup.TERRA, maxsize=1)
    async def get_exchange_rates(self) -> dict[TerraNativeToken, Decimal]:
        oracle_rates = await self.client.lcd.oracle.exchange_rates()
        rates = {TerraNativeToken(c.denom): Decimal(str(c.amount)) for c in oracle_rates.to_list()}
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
        return round(exchange_rates[to_coin] / exchange_rates[from_coin], MAX_PRECISION)
