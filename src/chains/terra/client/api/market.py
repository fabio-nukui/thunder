from __future__ import annotations

from decimal import Decimal

from utils.cache import CacheGroup, ttl_cache

from ...core import BaseMarketApi, TerraNativeToken, TerraTokenAmount
from ...denoms import LUNA, SDT

MARKET_PARAMETERS_TTL = 600


class MarketApi(BaseMarketApi):
    def get_amount_market(
        self,
        offer_amount: TerraTokenAmount,
        ask_denom: TerraNativeToken,
    ) -> TerraTokenAmount:
        """Get market swap amount, based on Terra implementation at
        https://github.com/terra-money/core/blob/v0.5.5/x/market/keeper/swap.go
        """
        if not isinstance(offer_amount.token, TerraNativeToken):
            raise TypeError('Market trades only available to native tokens')

        if LUNA not in (offer_amount.token, ask_denom):
            tobin_taxes = self.get_tobin_taxes()
            tobin_tax = max(tobin_taxes[offer_amount.token], tobin_taxes[ask_denom])
            ask_amount = self._compute_internal_swap(offer_amount, ask_denom)

            return ask_amount * (1 - tobin_tax)
        return self._get_amount_luna_market(offer_amount, ask_denom)

    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=MARKET_PARAMETERS_TTL)
    def get_tobin_taxes(self) -> dict[TerraNativeToken, Decimal]:
        result = self.client.lcd.oracle.parameters()
        return {
            TerraNativeToken(item['name']): Decimal(item['tobin_tax'])
            for item in result['whitelist']
        }

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=MARKET_PARAMETERS_TTL)
    def market_parameters(self) -> dict[str, Decimal]:
        return {
            k: Decimal(v)
            for k, v in self.client.lcd.market.parameters().items()
        }

    @ttl_cache(CacheGroup.TERRA, maxsize=1)
    def get_virtual_pools(self) -> tuple[Decimal, Decimal]:
        """Calculate virtual liquidity pool reserves in SDR
        See https://docs.terra.money/Reference/Terra-core/Module-specifications/spec-market.html#market-making-algorithm  # noqa: E501
        """
        base_bool = SDT.decimalize(self.market_parameters['base_pool'])
        terra_pool_delta = SDT.decimalize(str(self.client.lcd.market.terra_pool_delta()))

        pool_terra = base_bool + terra_pool_delta
        pool_luna = base_bool ** 2 / pool_terra

        return pool_terra, pool_luna

    def _get_amount_luna_market(
        self,
        offer_amount: TerraTokenAmount,
        ask_denom: TerraNativeToken,
    ) -> TerraTokenAmount:
        pool_terra, pool_luna = self.get_virtual_pools()
        if ask_denom == LUNA:
            pool_offer = pool_terra
            pool_ask = pool_luna
        else:
            pool_offer = pool_luna
            pool_ask = pool_terra
        base_offer_amount = self._compute_internal_swap(offer_amount, SDT)
        ret_amount = self._compute_internal_swap(base_offer_amount, ask_denom)

        ask_base_amount = pool_ask * (1 - pool_offer / (pool_offer + base_offer_amount.amount))
        spread = max(
            self.market_parameters['min_stability_spread'],
            (base_offer_amount.amount - ask_base_amount) / base_offer_amount.amount
        )
        return ask_denom.to_amount(ret_amount.amount * (1 - spread))

    def _compute_internal_swap(
        self,
        offer_amount: TerraTokenAmount,
        ask_denom: TerraNativeToken,
    ) -> TerraTokenAmount:
        return (
            ask_denom.to_amount(
                offer_amount.amount
                * self.client.oracle.exchange_rates[ask_denom]
                / self.client.oracle.exchange_rates[offer_amount.token]  # type: ignore
            )
        )
