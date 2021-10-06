from __future__ import annotations

from decimal import Decimal

from utils.cache import CacheGroup, ttl_cache

from ..core import BaseMarketApi, TerraNativeToken, TerraTokenAmount
from ..denoms import LUNA, SDT

MARKET_PARAMETERS_TTL = 600


class MarketApi(BaseMarketApi):
    def get_amount_out(
        self,
        offer_amount: TerraTokenAmount,
        ask_denom: TerraNativeToken,
    ) -> TerraTokenAmount:
        """Get market swap amount, based on Terra implementation at
        https://github.com/terra-money/core/blob/v0.5.5/x/market/keeper/swap.go
        """
        if not isinstance(offer_amount.token, TerraNativeToken):
            raise TypeError("Market trades only available to native tokens")

        if LUNA in (offer_amount.token, ask_denom):
            vp_terra, vp_luna = self.virtual_pools
            vp_offer, vp_ask = (vp_terra, vp_luna) if ask_denom == LUNA else (vp_luna, vp_terra)

            offer_amount_sdr = self._compute_swap_no_spread(offer_amount, SDT).amount
            ask_amount_sdr = vp_ask * (offer_amount_sdr / (offer_amount_sdr + vp_offer))
            vp_spread = (offer_amount_sdr - ask_amount_sdr) / offer_amount_sdr

            spread = max(vp_spread, self.market_parameters["min_stability_spread"])
        else:
            spread = max(self.tobin_taxes[offer_amount.token], self.tobin_taxes[ask_denom])

        ask_amount = self._compute_swap_no_spread(offer_amount, ask_denom)
        return ask_amount * (1 - spread)

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1)
    def virtual_pools(self) -> tuple[Decimal, Decimal]:
        """Calculate virtual liquidity pool reserves in SDR
        See https://docs.terra.money/Reference/Terra-core/Module-specifications/spec-market.html#market-making-algorithm  # noqa: E501
        """
        base_bool = SDT.decimalize(self.market_parameters["base_pool"])
        terra_pool_delta = SDT.decimalize(str(self.client.lcd.market.terra_pool_delta()))

        pool_terra = base_bool + terra_pool_delta
        pool_luna = base_bool ** 2 / pool_terra

        return pool_terra, pool_luna

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=MARKET_PARAMETERS_TTL)
    def tobin_taxes(self) -> dict[TerraNativeToken, Decimal]:
        result = self.client.lcd.oracle.parameters()
        return {
            TerraNativeToken(item["name"]): Decimal(item["tobin_tax"])
            for item in result["whitelist"]
        }

    @property
    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=MARKET_PARAMETERS_TTL)
    def market_parameters(self) -> dict[str, Decimal]:
        return {k: Decimal(v) for k, v in self.client.lcd.market.parameters().items()}

    def _compute_swap_no_spread(
        self,
        offer_amount: TerraTokenAmount,
        ask_denom: TerraNativeToken,
    ) -> TerraTokenAmount:
        return ask_denom.to_amount(
            offer_amount.amount
            * self.client.oracle.exchange_rates[ask_denom]
            / self.client.oracle.exchange_rates[offer_amount.token]  # type: ignore
        )
