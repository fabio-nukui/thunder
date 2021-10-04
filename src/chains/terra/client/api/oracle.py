from __future__ import annotations

from decimal import Decimal

from utils.cache import CacheGroup, ttl_cache

from ...core import LUNA, BaseTerraClient, TerraNativeToken

MAX_PRECISION = 18


class OracleApi:
    def __init__(self, client: BaseTerraClient):
        self.client = client

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1)
    def exchange_rates(self) -> dict[TerraNativeToken, Decimal]:
        rates = {
            TerraNativeToken(c.denom): Decimal(str(c.amount))
            for c in self.client.lcd.oracle.exchange_rates().to_list()
        }
        rates[LUNA] = Decimal(1)
        return rates

    def get_exchange_rate(
        self,
        from_coin: TerraNativeToken | str,
        to_coin: TerraNativeToken | str,
    ) -> Decimal:
        if isinstance(from_coin, str):
            from_coin = TerraNativeToken(from_coin)
        if isinstance(to_coin, str):
            to_coin = TerraNativeToken(to_coin)
        return round(self.exchange_rates[to_coin] / self.exchange_rates[from_coin], MAX_PRECISION)
