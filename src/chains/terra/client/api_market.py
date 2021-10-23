from __future__ import annotations

from decimal import Decimal

from utils.cache import CacheGroup, ttl_cache

from ..denoms import LUNA, SDT
from ..token import TerraNativeToken, TerraTokenAmount
from .base_api import Api

MARKET_PARAMETERS_TTL = 600


class MarketApi(Api):
    async def get_amount_out(
        self,
        offer_amount: TerraTokenAmount,
        ask_denom: TerraNativeToken,
        safety_margin: bool | int = False,
    ) -> TerraTokenAmount:
        """Get market swap amount, based on Terra implementation at
        https://github.com/terra-money/core/blob/v0.5.5/x/market/keeper/swap.go
        """
        if not isinstance(offer_amount.token, TerraNativeToken):
            raise TypeError("Market trades only available to native tokens")

        if LUNA in (offer_amount.token, ask_denom):
            vp_terra, vp_luna = await self.get_virtual_pools()
            vp_offer, vp_ask = (vp_terra, vp_luna) if ask_denom == LUNA else (vp_luna, vp_terra)

            offer_amount_sdr = (await self._compute_swap_no_spread(offer_amount, SDT)).amount
            ask_amount_sdr = vp_ask * (offer_amount_sdr / (offer_amount_sdr + vp_offer))
            vp_spread = (offer_amount_sdr - ask_amount_sdr) / offer_amount_sdr

            spread = max(vp_spread, await self.get_market_parameter("min_stability_spread"))
        else:
            tobin_taxes = await self.get_tobin_taxes()
            spread = max(tobin_taxes[offer_amount.token], tobin_taxes[ask_denom])

        ask_amount = await self._compute_swap_no_spread(offer_amount, ask_denom)
        return (ask_amount * (1 - spread)).safe_margin(safety_margin)

    @ttl_cache(CacheGroup.TERRA, maxsize=1)
    async def get_virtual_pools(self) -> tuple[Decimal, Decimal]:
        """Calculate virtual liquidity pool reserves in SDR
        See https://docs.terra.money/Reference/Terra-core/Module-specifications/spec-market.html#market-making-algorithm  # noqa: E501
        """
        base_bool = SDT.decimalize((await self.get_market_parameter("base_pool")))
        terra_pool_delta = SDT.decimalize(str(await self.client.lcd.market.terra_pool_delta()))

        pool_terra = base_bool + terra_pool_delta
        pool_luna = base_bool ** 2 / pool_terra

        return pool_terra, pool_luna

    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=MARKET_PARAMETERS_TTL)
    async def get_tobin_taxes(self) -> dict[TerraNativeToken, Decimal]:
        response = await self.client.lcd.oracle.parameters()
        return {
            TerraNativeToken(item["name"]): Decimal(item["tobin_tax"])
            for item in response["whitelist"]
        }

    async def get_market_parameter(self, param_name: str) -> Decimal:
        return (await self.get_market_parameters())[param_name]

    @ttl_cache(CacheGroup.TERRA, maxsize=1, ttl=MARKET_PARAMETERS_TTL)
    async def get_market_parameters(self) -> dict[str, Decimal]:
        params = await self.client.lcd.market.parameters()
        return {k: Decimal(v) for k, v in params.items()}

    async def _compute_swap_no_spread(
        self,
        offer_amount: TerraTokenAmount,
        ask_denom: TerraNativeToken,
    ) -> TerraTokenAmount:
        rates = await self.client.oracle.get_exchange_rates()
        return ask_denom.to_amount(
            offer_amount.amount * rates[ask_denom] / rates[offer_amount.token]  # type: ignore
        )
