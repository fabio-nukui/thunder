from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from cosmos_proto.terra.market.v1beta1 import QueryStub
from cosmos_sdk.core.market.msgs import MsgSwap

from utils.cache import CacheGroup, ttl_cache

from ...client.base_api import Api
from ..denoms import LUNA, SDT
from ..token import TerraNativeToken, TerraTokenAmount
from .utils import parse_proto_decimal

if TYPE_CHECKING:
    from .async_client import TerraClient  # noqa: F401

_MARKET_PARAMETERS_TTL = 3600
_DEFAULT_MIN_SAFETY_MARGIN = 100


class MarketApi(Api["TerraClient"]):
    def start(self):
        self.grpc_query = QueryStub(self.client.grpc_channel)

    async def get_amount_out(
        self,
        offer_amount: TerraTokenAmount,
        ask_denom: TerraNativeToken,
        safety_margin: bool | int = False,
        terra_pool_delta_change: Decimal = Decimal(0),
    ) -> TerraTokenAmount:
        """Get market swap amount, based on Terra implementation at
        https://github.com/terra-money/core/blob/v0.5.5/x/market/keeper/swap.go
        """
        if safety_margin:
            safety_margin = max(safety_margin, _DEFAULT_MIN_SAFETY_MARGIN)
        if not isinstance(offer_amount.token, TerraNativeToken):
            raise TypeError("Market trades only available to native tokens")

        if LUNA in (offer_amount.token, ask_denom):
            vp_terra, vp_luna = await self.get_virtual_pools(terra_pool_delta_change)
            vp_offer, vp_ask = (vp_terra, vp_luna) if ask_denom == LUNA else (vp_luna, vp_terra)

            offer_amount_sdr = (await self.compute_swap_no_spread(offer_amount, SDT)).amount
            ask_amount_sdr = vp_ask * (offer_amount_sdr / (offer_amount_sdr + vp_offer))
            vp_spread = (offer_amount_sdr - ask_amount_sdr) / offer_amount_sdr

            spread = max(vp_spread, await self.get_market_parameter("min_stability_spread"))
        else:
            tobin_taxes = await self.client.oracle.get_tobin_taxes()
            spread = max(tobin_taxes[offer_amount.token], tobin_taxes[ask_denom])

        ask_amount = await self.compute_swap_no_spread(offer_amount, ask_denom)
        return (ask_amount * (1 - spread)).safe_margin(safety_margin)

    async def get_simulation_amount_out(self, msg: MsgSwap) -> TerraTokenAmount:
        events = await self.client.tx.get_simulation_events([msg])
        (amount_out,) = [
            TerraTokenAmount.from_str(e["amount"])
            for e in events["coin_received"]
            if e["receiver"] == self.client.address
        ]
        return amount_out

    @ttl_cache(CacheGroup.TERRA)
    async def get_virtual_pools(
        self,
        terra_pool_delta_change: Decimal = Decimal(0),
    ) -> tuple[Decimal, Decimal]:
        """Calculate virtual liquidity pool reserves in SDR
        See https://docs.terra.money/Reference/Terra-core/Module-specifications/spec-market.html#market-making-algorithm  # noqa: E501
        """
        base_bool = SDT.decimalize(await self.get_market_parameter("base_pool"))
        res = await self.grpc_query.terra_pool_delta()
        pool_delta = parse_proto_decimal(res.terra_pool_delta)
        terra_pool_delta = SDT.decimalize(pool_delta) + terra_pool_delta_change

        pool_terra = base_bool + terra_pool_delta
        pool_luna = base_bool ** 2 / pool_terra

        return pool_terra, pool_luna

    async def get_market_parameter(self, param_name: str) -> Decimal:
        return (await self.get_market_parameters())[param_name]

    @ttl_cache(CacheGroup.TERRA, ttl=_MARKET_PARAMETERS_TTL)
    async def get_market_parameters(self) -> dict[str, Decimal]:
        res = await self.grpc_query.params()
        return {
            "base_pool": parse_proto_decimal(res.params.base_pool),
            "min_stability_spread": parse_proto_decimal(res.params.min_stability_spread),
            "pool_recovery_period": Decimal(res.params.pool_recovery_period),
        }

    async def compute_swap_no_spread(
        self,
        offer_amount: TerraTokenAmount,
        ask_denom: TerraNativeToken,
    ) -> TerraTokenAmount:
        rates = await self.client.oracle.get_exchange_rates()
        return ask_denom.to_amount(
            offer_amount.amount * rates[ask_denom] / rates[offer_amount.token]  # type: ignore
        )
