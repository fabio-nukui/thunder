from __future__ import annotations

import json

from ..client import TerraClient
from .factory import Factory
from .liquidity_pair import Action, LiquidityPair
from .router import Router, RouteStep, RouteStepNative, RouteStepTerraswap

__all__ = [
    "Action",
    "LiquidityPair",
    "RouteStep",
    "RouteStepNative",
    "RouteStepTerraswap",
    "Router",
    "TerraswapFactory",
    "LoopFactory",
]

ADDRESSES_FILE = "resources/addresses/terra/{chain_id}/terraswap/{name}.json"


def _get_addresses(chain_id: str, name: str) -> dict:
    return json.load(open(ADDRESSES_FILE.format(chain_id=chain_id, name=name)))


class TerraswapFactory(Factory):
    @classmethod
    async def new(cls, client: TerraClient) -> TerraswapFactory:
        addresses = _get_addresses(client.chain_id, "terraswap")
        return await super().new(client, addresses)


class LoopFactory(Factory):
    @classmethod
    async def new(cls, client: TerraClient) -> LoopFactory:
        addresses = _get_addresses(client.chain_id, "loop")
        return await super().new(client, addresses)
