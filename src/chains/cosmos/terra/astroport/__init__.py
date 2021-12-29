from __future__ import annotations

import json
from typing import TYPE_CHECKING

from cosmos_sdk.core import AccAddress

from .factory import Factory
from .liquidity_pair import LiquidityPair

if TYPE_CHECKING:
    from ..client import TerraClient

__all__ = [
    "LiquidityPair",
    "AstroportFactory",
]

ADDRESSES_FILE = "resources/addresses/cosmos/{chain_id}/astroport/{name}.json"


def _get_addresses(chain_id: str, name: str) -> dict:
    return json.load(open(ADDRESSES_FILE.format(chain_id=chain_id, name=name)))


class AstroportFactory(Factory):
    router_address: AccAddress

    @classmethod
    async def new(cls, client: TerraClient) -> AstroportFactory:  # type: ignore[override]
        addresses = _get_addresses(client.chain_id, "astroport")
        return await super().new(client, addresses, "astroport")
