from __future__ import annotations

import json
from typing import TYPE_CHECKING

from cosmos_sdk.core import AccAddress

from .factory import Factory
from .liquidity_pair import Action, LiquidityPair, RouterNativeLiquidityPair
from .router import Router, RouterLiquidityPair, RouteStep, RouteStepNative, RouteStepTerraswap
from .utils import EncodingVersion

if TYPE_CHECKING:
    from ..client import TerraClient

__all__ = [
    "Action",
    "LiquidityPair",
    "RouterNativeLiquidityPair",
    "Router",
    "RouterLiquidityPair",
    "RouteStep",
    "RouteStepNative",
    "RouteStepTerraswap",
    "TerraswapFactory",
    "LoopFactory",
]

ADDRESSES_FILE = "resources/addresses/cosmos/{chain_id}/terraswap/{name}.json"


def _get_addresses(chain_id: str, name: str) -> dict:
    return json.load(open(ADDRESSES_FILE.format(chain_id=chain_id, name=name)))


class TerraswapFactory(Factory):
    router_address: AccAddress

    @classmethod
    async def new(cls, client: TerraClient) -> TerraswapFactory:  # type: ignore[override]
        addresses = _get_addresses(client.chain_id, "terraswap")
        return await super().new(
            client=client,
            addresses=addresses,
            name="terraswap",
            router_swap_action="terra_swap",
        )


class LoopFactory(Factory):
    @classmethod
    async def new(cls, client: TerraClient) -> LoopFactory:  # type: ignore[override]
        addresses = _get_addresses(client.chain_id, "loop")
        return await super().new(client=client, addresses=addresses, name="loop")


class PrismFactory(Factory):
    @classmethod
    async def new(cls, client: TerraClient) -> PrismFactory:  # type: ignore[override]
        addresses = _get_addresses(client.chain_id, "prism")
        return await super().new(
            client=client,
            addresses=addresses,
            name="prism",
            router_swap_action="prism_swap",
            encoding_version=EncodingVersion.v2,
        )
